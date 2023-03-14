/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.delete

import cats.effect.{Async, Temporal}
import cats.syntax.all._
import io.renku.events.consumers.Project
import io.renku.graph.eventlog
import io.renku.graph.eventlog.api.events.CommitSyncRequest
import io.renku.graph.model.projects
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.security.model.AuthUser
import io.renku.http.InfoMessage._
import io.renku.metrics.MetricsRegistry
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait Endpoint[F[_]] {
  def `DELETE /projects/:path`(path: projects.Path, authUser: AuthUser): F[Response[F]]
}

object Endpoint {
  def apply[F[_]: Async: Logger: MetricsRegistry: GitLabClient]: F[Endpoint[F]] =
    eventlog.api.events
      .Client[F]
      .map(new EndpointImpl(ProjectFinder[F], ProjectRemover[F], _))
}

private class EndpointImpl[F[_]: Temporal: Logger](projectFinder: ProjectFinder[F],
                                                   projectRemover:      ProjectRemover[F],
                                                   elClient:            eventlog.api.events.Client[F],
                                                   waitBeforeNextCheck: Duration = 1 second
) extends Http4sDsl[F]
    with Endpoint[F] {

  import elClient.send
  import projectFinder.findProject
  import projectRemover.deleteProject

  override def `DELETE /projects/:path`(path: projects.Path, authUser: AuthUser): F[Response[F]] = {
    implicit val at: AccessToken = authUser.accessToken

    findProject(path) >>= {
      case None =>
        NotFound(InfoMessage("Project does not exist"))
      case Some(project) =>
        deleteProject(project.id) >>
          waitForDeletion(project) >>
          send(CommitSyncRequest(project)) >>
          Accepted(InfoMessage("Project deleted"))
    }
  }.handleErrorWith(httpResult(path))

  private def waitForDeletion(project: Project)(implicit ac: AccessToken): F[Unit] =
    findProject(project.path) >>= {
      case None    => ().pure[F]
      case Some(_) => Temporal[F].delayBy(waitForDeletion(project), waitBeforeNextCheck)
    }

  private def httpResult(path: projects.Path): Throwable => F[Response[F]] = { exception =>
    Logger[F].error(exception)(show"Deleting '$path' project failed") >>
      InternalServerError(ErrorMessage(s"Project deletion failure: ${exception.getMessage}"))
  }
}
