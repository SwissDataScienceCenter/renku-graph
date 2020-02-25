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

import cats.effect._
import cats.implicits._
import ch.datascience.config.renku
import ch.datascience.control.Throttler
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.graph.model.projects
import ch.datascience.http.rest.Links.{Href, Link, Rel, _links}
import ch.datascience.knowledgegraph.config.GitLab
import ch.datascience.knowledgegraph.projects.model.{Creator, Project, RepoUrls}
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import io.chrisdavenport.log4cats.Logger
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class ProjectEndpoint[Interpretation[_]: Effect](
    projectFinder:         ProjectFinder[Interpretation],
    renkuResourcesUrl:     renku.ResourcesUrl,
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation]
) extends Http4sDsl[Interpretation] {

  import executionTimeRecorder._
  import io.circe.literal._
  import io.circe.syntax._
  import io.circe.{Encoder, Json}
  import org.http4s.circe._

  def getProject(path: projects.Path): Interpretation[Response[Interpretation]] =
    measureExecutionTime {
      projectFinder
        .findProject(path)
        .flatMap(toHttpResult(path))
        .recoverWith(httpResult(path))
    } map logExecutionTimeWhen(finishedSuccessfully(path))

  private def toHttpResult(
      path: projects.Path
  ): Option[Project] => Interpretation[Response[Interpretation]] = {
    case None          => NotFound(InfoMessage(s"No '$path' project found"))
    case Some(project) => Ok(project.asJson)
  }

  private def httpResult(
      path: projects.Path
  ): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage(s"Finding '$path' project failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(projectPath: projects.Path): PartialFunction[Response[Interpretation], String] = {
    case response if response.status == Ok || response.status == NotFound =>
      s"Finding '$projectPath' details finished"
  }

  private implicit lazy val projectEncoder: Encoder[Project] = Encoder.instance[Project] { project =>
    json"""{
        "identifier": ${project.id.value},
        "path":       ${project.path.value},
        "name":       ${project.name.value},
        "visibility": ${project.visibility.value},
        "created": {
          "dateCreated": ${project.created.date.value},
          "creator":     ${project.created.creator}
        },
        "url":        ${project.repoUrls},
        "forksCount": ${project.forksCount.value},
        "starsCount": ${project.starsCount.value},
        "updatedAt":  ${project.updatedAt.value}
    }""" deepMerge _links(
      Link(Rel.Self        -> Href(renkuResourcesUrl / "projects" / project.path)),
      Link(Rel("datasets") -> Href(renkuResourcesUrl / "projects" / project.path / "datasets"))
    ) deepMerge (project.maybeDescription.map(desc => json"""{"description": ${desc.value}}""") getOrElse Json.obj())
  }

  private implicit lazy val creatorEncoder: Encoder[Creator] = Encoder.instance[Creator] { creator =>
    json"""{
      "name":  ${creator.name.value},
      "email": ${creator.email.value}
    }"""
  }

  private implicit lazy val urlsEncoder: Encoder[RepoUrls] = Encoder.instance[RepoUrls] { urls =>
    json"""{
      "ssh":  ${urls.ssh.value},
      "http": ${urls.http.value}
    }"""
  }
}

object IOProjectEndpoint {

  def apply(
      gitLabThrottler:         Throttler[IO, GitLab],
      timeRecorder:            SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[ProjectEndpoint[IO]] =
    for {
      projectFinder         <- IOProjectFinder(gitLabThrottler, ApplicationLogger, timeRecorder)
      renkuResourceUrl      <- renku.ResourcesUrl[IO]()
      executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger)
    } yield new ProjectEndpoint[IO](
      projectFinder,
      renkuResourceUrl,
      executionTimeRecorder,
      ApplicationLogger
    )
}
