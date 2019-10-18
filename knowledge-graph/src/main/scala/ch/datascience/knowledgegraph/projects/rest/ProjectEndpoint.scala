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

import cats.effect._
import cats.implicits._
import ch.datascience.config.RenkuResourcesUrl
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.knowledgegraph.projects.model.{Project, ProjectCreator}
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import io.chrisdavenport.log4cats.Logger
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class ProjectEndpoint[Interpretation[_]: Effect](
    projectFinder:         ProjectFinder[Interpretation],
    renkuResourcesUrl:     RenkuResourcesUrl,
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation]
) extends Http4sDsl[Interpretation] {

  import executionTimeRecorder._
  import org.http4s.circe._

  def getProject(path: ProjectPath): Interpretation[Response[Interpretation]] =
    measureExecutionTime {
      projectFinder
        .findProject(path)
        .flatMap(toHttpResult(path))
        .recoverWith(httpResult(path))
    } map logExecutionTimeWhen(finishedSuccessfully(path))

  private def toHttpResult(
      path: ProjectPath
  ): Option[Project] => Interpretation[Response[Interpretation]] = {
    case None          => NotFound(InfoMessage(s"No '$path' project found"))
    case Some(project) => Ok(project.asJson)
  }

  private def httpResult(
      path: ProjectPath
  ): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage(s"Finding '$path' project failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(projectPath: ProjectPath): PartialFunction[Response[Interpretation], String] = {
    case response if response.status == Ok || response.status == NotFound =>
      s"Finding '$projectPath' details finished"
  }

  private implicit lazy val projectEncoder: Encoder[Project] = Encoder.instance[Project] { project =>
    json"""{
        "path": ${project.path.toString},
        "name": ${project.name.toString},
        "created": {
          "dateCreated": ${project.created.date.toString},
          "creator": ${project.created.creator}
        }
    }""" deepMerge _links(
      Link(Rel.Self        -> Href(renkuResourcesUrl / "projects" / project.path)),
      Link(Rel("datasets") -> Href(renkuResourcesUrl / "projects" / project.path / "datasets"))
    )
  }

  private implicit lazy val creatorEncoder: Encoder[ProjectCreator] = Encoder.instance[ProjectCreator] { creator =>
    json"""{
      "name": ${creator.name.toString},
      "email": ${creator.email.toString}
    }"""
  }
}

object IOProjectEndpoint {

  def apply()(implicit executionContext: ExecutionContext,
              contextShift:              ContextShift[IO],
              timer:                     Timer[IO]): IO[ProjectEndpoint[IO]] =
    for {
      projectFinder         <- IOProjectFinder(logger = ApplicationLogger)
      renkuResourceUrl      <- RenkuResourcesUrl[IO]()
      executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger)
    } yield
      new ProjectEndpoint[IO](
        projectFinder,
        renkuResourceUrl,
        executionTimeRecorder,
        ApplicationLogger
      )
}
