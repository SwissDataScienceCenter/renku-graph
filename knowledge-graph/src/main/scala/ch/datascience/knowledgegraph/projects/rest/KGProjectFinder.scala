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

package ch.datascience.knowledgegraph.projects.rest

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects._
import ch.datascience.graph.model.users
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder._
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait KGProjectFinder[Interpretation[_]] {
  def findProject(path: Path): Interpretation[Option[KGProject]]
}

object KGProjectFinder {

  final case class KGProject(path: Path, name: Name, created: ProjectCreation, maybeParent: Option[Parent])

  final case class ProjectCreation(date: DateCreated, maybeCreator: Option[ProjectCreator])

  final case class Parent(resourceId: ResourceId, name: Name, created: ProjectCreation)

  final case class ProjectCreator(maybeEmail: Option[users.Email], name: users.Name)
}

private class IOKGProjectFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext,
  contextShift:              ContextShift[IO],
  timer:                     Timer[IO],
  ME:                        MonadError[IO, Throwable])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with KGProjectFinder[IO] {

  import cats.implicits._
  import eu.timepit.refined.auto._
  import io.circe.Decoder

  override def findProject(path: Path): IO[Option[KGProject]] = {
    implicit val decoder: Decoder[List[KGProject]] = recordsDecoder(path)
    queryExpecting[List[KGProject]](using = query(path)) flatMap toSingleProject
  }

  private def query(path: Path) = SparqlQuery(
    name = "project by id",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>"
    ),
    s"""|SELECT DISTINCT ?name ?dateCreated ?maybeCreatorName ?maybeCreatorEmail ?maybeParentId ?maybeParentName ?maybeParentDateCreated ?maybeParentCreatorName ?maybeParentCreatorEmail
        |WHERE {
        |  BIND (${ResourceId(renkuBaseUrl, path).showAs[RdfResource]} AS ?projectId)
        |  ?projectId rdf:type <http://schema.org/Project>;
        |             schema:name ?name;
        |             schema:dateCreated ?dateCreated.
        |  OPTIONAL {
        |    ?projectId schema:creator ?maybeCreatorId.
        |    ?maybeCreatorId rdf:type <http://schema.org/Person>;
        |                    schema:name ?maybeCreatorName.
        |    OPTIONAL { ?maybeCreatorId schema:email ?maybeCreatorEmail }
        |  }
        |  OPTIONAL { 
        |    ?projectId prov:wasDerivedFrom ?maybeParentId.
        |    ?maybeParentId rdf:type <http://schema.org/Project>;
        |                   schema:name ?maybeParentName;
        |                   schema:dateCreated ?maybeParentDateCreated.
        |    OPTIONAL {
        |      ?maybeParentId schema:creator ?maybeParentCreatorId.
        |      ?maybeParentCreatorId rdf:type <http://schema.org/Person>;
        |                            schema:name ?maybeParentCreatorName.
        |      OPTIONAL { ?maybeParentCreatorId schema:email ?maybeParentCreatorEmail }
        |    }
        |  }
        |}
        |""".stripMargin
  )

  private def recordsDecoder(path: Path): Decoder[List[KGProject]] = {
    import Decoder._
    import ch.datascience.graph.model.projects._
    import ch.datascience.graph.model.users
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    val project: Decoder[KGProject] = { cursor =>
      for {
        name                   <- cursor.downField("name").downField("value").as[Name]
        dateCreated            <- cursor.downField("dateCreated").downField("value").as[DateCreated]
        maybeCreatorName       <- cursor.downField("maybeCreatorName").downField("value").as[Option[users.Name]]
        maybeCreatorEmail      <- cursor.downField("maybeCreatorEmail").downField("value").as[Option[users.Email]]
        maybeParentId          <- cursor.downField("maybeParentId").downField("value").as[Option[ResourceId]]
        maybeParentName        <- cursor.downField("maybeParentName").downField("value").as[Option[Name]]
        maybeParentDateCreated <- cursor.downField("maybeParentDateCreated").downField("value").as[Option[DateCreated]]
        maybeParentCreatorName <- cursor.downField("maybeParentCreatorName").downField("value").as[Option[users.Name]]
        maybeParentCreatorEmail <- cursor
                                    .downField("maybeParentCreatorEmail")
                                    .downField("value")
                                    .as[Option[users.Email]]
      } yield KGProject(
        path,
        name,
        ProjectCreation(dateCreated, maybeCreatorName map (name => ProjectCreator(maybeCreatorEmail, name))),
        maybeParent = (maybeParentId, maybeParentName, maybeParentDateCreated) mapN {
          case (parentId, name, dateCreated) =>
            Parent(parentId,
                   name,
                   ProjectCreation(dateCreated,
                                   maybeParentCreatorName.map(name => ProjectCreator(maybeParentCreatorEmail, name))))
        }
      )
    }

    _.downField("results").downField("bindings").as(decodeList(project))
  }

  private lazy val toSingleProject: List[KGProject] => IO[Option[KGProject]] = {
    case Nil            => ME.pure(None)
    case project +: Nil => ME.pure(Some(project))
    case projects       => ME.raiseError(new RuntimeException(s"More than one project with ${projects.head.path} path"))
  }
}

private object IOKGProjectFinder {

  def apply(
      timeRecorder:            SparqlQueryTimeRecorder[IO],
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO] = ApplicationLogger
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[KGProjectFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield new IOKGProjectFinder(config, renkuBaseUrl, logger, timeRecorder)
}
