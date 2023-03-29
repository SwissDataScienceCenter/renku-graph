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

package io.renku.knowledgegraph.projects.details

import cats.{MonadThrow, Parallel}
import cats.effect._
import cats.syntax.all._
import io.renku.config.renku
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.{projects, GitLabUrl}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.http.InfoMessage._
import io.renku.http.client.GitLabClient
import io.renku.http.rest.Links.Href
import io.renku.http.server.security.model.AuthUser
import io.renku.jsonld.syntax._
import io.renku.logging.ExecutionTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import io.renku.triplesstore.SparqlQueryTimeRecorder
import model._
import org.http4s.{Request, Response}
import org.http4s.MediaType.application
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.util.control.NonFatal

trait Endpoint[F[_]] {
  def `GET /projects/:path`(path: projects.Path, maybeAuthUser: Option[AuthUser])(implicit
      request: Request[F]
  ): F[Response[F]]
}

class EndpointImpl[F[_]: MonadThrow: Logger](
    projectFinder:         ProjectFinder[F],
    projectJsonEncoder:    ProjectJsonEncoder,
    projectJsonLDEncoder:  ProjectJsonLDEncoder,
    tgClient:              triplesgenerator.api.events.Client[F],
    executionTimeRecorder: ExecutionTimeRecorder[F],
    gitLabUrl:             GitLabUrl,
    now:                   () => Instant = () => Instant.now()
) extends Http4sDsl[F]
    with Endpoint[F] {

  import executionTimeRecorder._
  import io.circe.syntax._
  import io.renku.http.jsonld4s._
  import io.renku.http.server.endpoint._
  import org.http4s.circe.jsonEncoder

  private implicit lazy val glUrl: GitLabUrl = gitLabUrl

  def `GET /projects/:path`(path: projects.Path, maybeAuthUser: Option[AuthUser])(implicit
      request: Request[F]
  ): F[Response[F]] = measureExecutionTime {
    projectFinder
      .findProject(path, maybeAuthUser)
      .flatTap(sendProjectViewedEvent(maybeAuthUser))
      .flatMap(toHttpResult(path))
      .recoverWith(httpResult(path))
  } map logExecutionTimeWhen(finishedSuccessfully(path))

  private def sendProjectViewedEvent(maybeAuthUser: Option[AuthUser]): Option[Project] => F[Unit] = {
    case None => ().pure[F]
    case Some(proj) =>
      tgClient
        .send(ProjectViewedEvent.forProjectAndUserId(proj.path, maybeAuthUser.map(_.id), now))
        .handleErrorWith(err => Logger[F].error(err)(show"sending ${ProjectViewedEvent.categoryName} event failed"))
  }

  private def toHttpResult(path: projects.Path)(implicit request: Request[F]): Option[Project] => F[Response[F]] = {
    case None =>
      val message = InfoMessage(s"No '$path' project found")
      whenAccept(
        application.`ld+json` --> NotFound(message.asJsonLD),
        application.json      --> NotFound(message.asJson)
      )(default = NotFound(message.asJson))
    case Some(project) =>
      whenAccept(
        application.`ld+json` --> Ok(projectJsonLDEncoder encode project),
        application.json      --> Ok(projectJsonEncoder encode project)
      )(default = Ok(projectJsonEncoder encode project))
  }

  private def httpResult(path: projects.Path)(implicit
      request: Request[F]
  ): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val message = ErrorMessage(s"Finding '$path' project failed")
    Logger[F].error(exception)(message.value) >> whenAccept(
      application.`ld+json` --> InternalServerError(message.asJsonLD),
      application.json      --> InternalServerError(message.asJson)
    )(default = InternalServerError(message.asJson))
  }

  private def finishedSuccessfully(projectPath: projects.Path): PartialFunction[Response[F], String] = {
    case response if response.status == Ok || response.status == NotFound =>
      s"Finding '$projectPath' details finished"
  }
}

object Endpoint {

  def apply[F[_]: Parallel: Async: GitLabClient: AccessTokenFinder: Logger: SparqlQueryTimeRecorder: MetricsRegistry]
      : F[Endpoint[F]] =
    for {
      projectFinder         <- ProjectFinder[F]
      jsonEncoder           <- ProjectJsonEncoder[F]
      tgClient              <- triplesgenerator.api.events.Client[F]
      executionTimeRecorder <- ExecutionTimeRecorder[F]()
      gitLabUrl             <- GitLabUrlLoader[F]()
    } yield new EndpointImpl[F](projectFinder,
                                jsonEncoder,
                                ProjectJsonLDEncoder,
                                tgClient,
                                executionTimeRecorder,
                                gitLabUrl
    )

  def href(renkuApiUrl: renku.ApiUrl, projectPath: projects.Path): Href =
    Href(renkuApiUrl / "projects" / projectPath)
}
