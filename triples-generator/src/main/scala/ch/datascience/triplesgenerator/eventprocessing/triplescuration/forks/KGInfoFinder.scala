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
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
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

  override def findProject(path: Path): IO[Option[KGProject]] = {
    implicit val decoder: Decoder[List[KGProject]] = recordsDecoder(path)
    queryExpecting[List[KGProject]](using = query(path)) flatMap toSingleResult(path)
  }

  private def query(path: Path) = SparqlQuery(
    name = "upload - project by id",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>"
    ),
    s"""|SELECT DISTINCT ?projectId ?maybeParentProjectId ?dateCreated ?creatorResourceId ?maybeCreatorName ?maybeCreatorEmail
        |WHERE {
        |    BIND (${ResourceId(renkuBaseUrl, path).showAs[RdfResource]} AS ?projectId)
        |    ?projectId rdf:type <http://schema.org/Project>;
        |               schema:dateCreated ?dateCreated;
        |               schema:creator ?creatorResourceId.
        |    ?creatorResourceId rdf:type <http://schema.org/Person>.
        |    OPTIONAL { ?creatorResourceId schema:name ?maybeCreatorName }
        |    OPTIONAL { ?creatorResourceId schema:email ?maybeCreatorEmail }
        |    OPTIONAL { ?projectId prov:wasDerivedFrom ?maybeParentProjectId }
        |}
        |""".stripMargin
  )

  private def recordsDecoder(path: Path): Decoder[List[KGProject]] = {
    import Decoder._
    import ch.datascience.graph.model.projects._
    import ch.datascience.graph.model.users.{Email, Name => UserName}
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    val project: Decoder[KGProject] = { cursor =>
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

    _.downField("results").downField("bindings").as(decodeList(project))
  }

  private def toSingleResult(path: Path): List[KGProject] => IO[Option[KGProject]] = {
    case Nil            => ME.pure(None)
    case project +: Nil => ME.pure(Some(project))
    case _              => ME.raiseError(new RuntimeException(s"More than one project find for '$path' path"))
  }

  override def findCreatorId(email: users.Email): IO[Option[users.ResourceId]] = IO.pure(None)
}

private object IOKGInfoFinder {
  def apply(
      timeRecorder:            SparqlQueryTimeRecorder[IO],
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO] = ApplicationLogger
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[KGInfoFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield new IOKGInfoFinder(config, renkuBaseUrl, logger, timeRecorder)
}
