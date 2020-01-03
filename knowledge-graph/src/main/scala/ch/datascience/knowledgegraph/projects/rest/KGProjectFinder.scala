/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait KGProjectFinder[Interpretation[_]] {
  def findProject(path: ProjectPath): Interpretation[Option[KGProject]]
}

object KGProjectFinder {
  final case class KGProject(path: ProjectPath, name: Name, created: ProjectCreation)

  final case class ProjectCreation(date: DateCreated, creator: ProjectCreator)

  final case class ProjectCreator(email: users.Email, name: users.Name)
}

private class IOKGProjectFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext,
  contextShift:              ContextShift[IO],
  timer:                     Timer[IO],
  ME:                        MonadError[IO, Throwable])
    extends IORdfStoreClient(rdfStoreConfig, logger)
    with KGProjectFinder[IO] {

  import io.circe.Decoder

  override def findProject(path: ProjectPath): IO[Option[KGProject]] = {
    implicit val decoder: Decoder[List[KGProject]] = recordsDecoder(path)
    queryExpecting[List[KGProject]](using = query(path)) flatMap toSingleProject
  }

  private def query(path: ProjectPath): String =
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX schema: <http://schema.org/>
       |PREFIX dcterms: <http://purl.org/dc/terms/>
       |
       |SELECT DISTINCT ?name ?dateCreated ?creatorName ?creatorEmail
       |WHERE {
       |  ${ProjectResource(renkuBaseUrl, path).showAs[RdfResource]} rdf:type <http://schema.org/Project> ;
       |                                                             schema:name ?name ;
       |                                                             schema:dateCreated ?dateCreated ;
       |                                                             schema:creator ?creatorResource .
       |  ?creatorResource rdf:type <http://schema.org/Person> ;
       |                   schema:email ?creatorEmail ;
       |                   schema:name ?creatorName .         
       |}""".stripMargin

  private def recordsDecoder(path: ProjectPath): Decoder[List[KGProject]] = {
    import Decoder._
    import ch.datascience.graph.model.projects._
    import ch.datascience.graph.model.users.{Email, Name => UserName}
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    val project: Decoder[KGProject] = { cursor =>
      for {
        name         <- cursor.downField("name").downField("value").as[Name]
        dateCreated  <- cursor.downField("dateCreated").downField("value").as[DateCreated]
        creatorName  <- cursor.downField("creatorName").downField("value").as[UserName]
        creatorEmail <- cursor.downField("creatorEmail").downField("value").as[Email]
      } yield KGProject(
        path,
        name,
        ProjectCreation(dateCreated, ProjectCreator(creatorEmail, creatorName))
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
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO] = ApplicationLogger
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[KGProjectFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield new IOKGProjectFinder(config, renkuBaseUrl, logger)
}
