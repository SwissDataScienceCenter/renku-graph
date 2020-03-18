/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.model.users
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait KGInfoFinder[Interpretation[_]] {
  def findProject(path:    Path):        Interpretation[Option[KGProject]]
  def findCreatorId(email: users.Email): Interpretation[Option[users.ResourceId]]
}

private class IOKGInfoFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext,
  contextShift:              ContextShift[IO],
  timer:                     Timer[IO],
  ME:                        MonadError[IO, Throwable])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with KGInfoFinder[IO] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import Decoder._
  import ch.datascience.graph.model.projects._
  import ch.datascience.graph.model.users.{Email, Name => UserName}
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import SparqlValueEncoder.sparqlEncode

  override def findProject(path: Path): IO[Option[KGProject]] = {
    implicit val decoder: Decoder[List[KGProject]] = recordsDecoder(projectDecoder)
    queryExpecting[List[KGProject]](using = projectFindingQuery(path)) flatMap toSingleResult(path)
  }

  private def projectFindingQuery(path: Path) = SparqlQuery(
    name = "upload - project by path",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>"
    ),
    s"""|SELECT DISTINCT ?projectId ?maybeParentProjectId ?dateCreated ?creatorResourceId ?maybeCreatorName ?maybeCreatorEmail
        |WHERE {
        |  BIND (${ResourceId(renkuBaseUrl, path).showAs[RdfResource]} AS ?projectId)
        |  ?projectId rdf:type <http://schema.org/Project>;
        |             schema:dateCreated ?dateCreated;
        |             schema:creator ?creatorResourceId.
        |  ?creatorResourceId rdf:type <http://schema.org/Person>.
        |  OPTIONAL { ?creatorResourceId schema:name ?maybeCreatorName }
        |  OPTIONAL { ?creatorResourceId schema:email ?maybeCreatorEmail }
        |  OPTIONAL { ?projectId prov:wasDerivedFrom ?maybeParentProjectId }
        |}
        |""".stripMargin
  )

  override def findCreatorId(email: users.Email): IO[Option[users.ResourceId]] = {
    implicit val decoder: Decoder[List[users.ResourceId]] = recordsDecoder(resourceIdDecoder)
    queryExpecting[List[users.ResourceId]](using = personIdFindingQuery(email)) flatMap toSingleResult(email)
  }

  private def personIdFindingQuery(email: Email) = SparqlQuery(
    name = "upload - personId by email",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?id
        |WHERE {
        |  ?id rdf:type <http://schema.org/Person>;
        |      schema:email "${sparqlEncode(email.value)}".
        |}
        |""".stripMargin
  )

  private def recordsDecoder[T](rowDecoder: Decoder[T]): Decoder[List[T]] =
    _.downField("results").downField("bindings").as(decodeList(rowDecoder))

  private val projectDecoder: Decoder[KGProject] = { cursor =>
    for {
      resourceId           <- cursor.downField("projectId").downField("value").as[ResourceId]
      maybeParentProjectId <- cursor.downField("maybeParentProjectId").downField("value").as[Option[ResourceId]]
      dateCreated          <- cursor.downField("dateCreated").downField("value").as[DateCreated]
      creatorResourceId    <- cursor.downField("creatorResourceId").downField("value").as[users.ResourceId]
      maybeCreatorName     <- cursor.downField("maybeCreatorName").downField("value").as[Option[users.Name]]
      maybeCreatorEmail    <- cursor.downField("maybeCreatorEmail").downField("value").as[Option[Email]]
    } yield KGProject(
      resourceId,
      maybeParentProjectId,
      KGCreator(creatorResourceId, maybeCreatorEmail, maybeCreatorName),
      dateCreated
    )
  }

  private val resourceIdDecoder: Decoder[users.ResourceId] =
    _.downField("id").downField("value").as[users.ResourceId]

  private def toSingleResult[V, O](value: V): List[O] => IO[Option[O]] = {
    case Nil           => ME.pure(None)
    case record +: Nil => ME.pure(Some(record))
    case _             => ME.raiseError(new RuntimeException(s"More than one record found for '$value'"))
  }
}

private object IOKGInfoFinder {
  def apply(
      timeRecorder:            SparqlQueryTimeRecorder[IO],
      logger:                  Logger[IO],
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO]()
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[KGInfoFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield new IOKGInfoFinder(config, renkuBaseUrl, logger, timeRecorder)
}
