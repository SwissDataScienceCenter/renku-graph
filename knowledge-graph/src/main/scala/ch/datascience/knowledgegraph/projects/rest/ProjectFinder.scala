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
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait ProjectFinder[Interpretation[_]] {
  def findProject(path: ProjectPath): Interpretation[Option[Project]]
}

private class IOProjectFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext,
  contextShift:              ContextShift[IO],
  timer:                     Timer[IO],
  ME:                        MonadError[IO, Throwable])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger)
    with ProjectFinder[IO] {

  import io.circe.Decoder

  override def findProject(path: ProjectPath): IO[Option[Project]] = {
    implicit val decoder: Decoder[List[Project]] = recordsDecoder(path)
    queryExpecting[List[Project]](using = query(path)) flatMap toSingleProject
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
       |  ${FullProjectPath(renkuBaseUrl, path).showAs[RdfResource]} rdf:type <http://schema.org/Project> ;
       |                                                             schema:name ?name ;
       |                                                             schema:dateCreated ?dateCreated ;
       |                                                             schema:creator ?creatorResource .
       |  ?creatorResource rdf:type <http://schema.org/Person> ;
       |                   schema:email ?creatorEmail ;
       |                   schema:name ?creatorName .         
       |}""".stripMargin

  private def recordsDecoder(path: ProjectPath): Decoder[List[Project]] = {
    import Decoder._
    import ch.datascience.graph.model.projects._
    import ch.datascience.graph.model.users.{Email, Name => UserName}
    import ch.datascience.knowledgegraph.projects.model._
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    val project: Decoder[Project] = { cursor =>
      for {
        name         <- cursor.downField("name").downField("value").as[Name]
        dateCreated  <- cursor.downField("dateCreated").downField("value").as[DateCreated]
        creatorName  <- cursor.downField("creatorName").downField("value").as[UserName]
        creatorEmail <- cursor.downField("creatorEmail").downField("value").as[Email]
      } yield
        Project(
          path,
          name,
          ProjectCreation(dateCreated, ProjectCreator(creatorEmail, creatorName))
        )
    }

    _.downField("results").downField("bindings").as(decodeList(project))
  }

  private lazy val toSingleProject: List[Project] => IO[Option[Project]] = {
    case Nil            => ME.pure(None)
    case project +: Nil => ME.pure(Some(project))
    case projects       => ME.raiseError(new RuntimeException(s"More than one project with ${projects.head.path} path"))
  }
}

private object IOProjectFinder {

  def apply(
      rdfStoreConfig:          IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      renkuBaseUrl:            IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      logger:                  Logger[IO] = ApplicationLogger
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[ProjectFinder[IO]] =
    for {
      config       <- rdfStoreConfig
      renkuBaseUrl <- renkuBaseUrl
    } yield new IOProjectFinder(config, renkuBaseUrl, logger)
}
