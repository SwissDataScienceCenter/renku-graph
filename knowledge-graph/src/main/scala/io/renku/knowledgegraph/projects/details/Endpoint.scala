/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.effect._
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import com.typesafe.config.ConfigFactory
import io.renku.config.renku
import io.renku.data.Message
import io.renku.data.MessageJsonLDEncoder._
import io.renku.graph.model.projects
import io.renku.http.client.{GitLabClient, GitLabClientLoader, GitLabUrl}
import io.renku.http.rest.Links.Href
import io.renku.http.server.security.model.AuthUser
import io.renku.jsonld.syntax._
import io.renku.logging.{ExecutionTimeRecorder, ExecutionTimeRecorderLoader}
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import io.renku.triplesstore.SparqlQueryTimeRecorder
import model._
import org.http4s.MediaType.application
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.util.control.NonFatal

trait Endpoint[F[_]] {
  def `GET /projects/:slug`(slug: projects.Slug, maybeAuthUser: Option[AuthUser])(implicit
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

  def `GET /projects/:slug`(slug: projects.Slug, maybeAuthUser: Option[AuthUser])(implicit
      request: Request[F]
  ): F[Response[F]] = measureAndLogTime(finishedSuccessfully(slug)) {
    projectFinder
      .findProject(slug, maybeAuthUser)
      .flatTap(sendProjectViewedEvent(maybeAuthUser))
      .flatMap(toHttpResult(slug))
      .recoverWith(httpResult(slug))
  }

  private def sendProjectViewedEvent(maybeAuthUser: Option[AuthUser]): Option[Project] => F[Unit] = {
    case None => ().pure[F]
    case Some(proj) =>
      tgClient
        .send(ProjectViewedEvent.forProjectAndUserId(proj.slug, maybeAuthUser.map(_.id), now))
        .handleErrorWith(err => Logger[F].error(err)(show"sending ${ProjectViewedEvent.categoryName} event failed"))
  }

  private def toHttpResult(slug: projects.Slug)(implicit request: Request[F]): Option[Project] => F[Response[F]] = {
    case None =>
      val message = Message.Info.unsafeApply(s"No '$slug' project found")
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

  private def httpResult(slug: projects.Slug)(implicit
      request: Request[F]
  ): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val message = Message.Error.unsafeApply(s"Finding '$slug' project failed")
    Logger[F].error(exception)(message.show) >> whenAccept(
      application.`ld+json` --> InternalServerError(message.asJsonLD),
      application.json      --> InternalServerError(message.asJson)
    )(default = InternalServerError(message.asJson))
  }

  private def finishedSuccessfully(projectSlug: projects.Slug): PartialFunction[Response[F], String] = {
    case response if response.status == Ok || response.status == NotFound =>
      s"Finding '$projectSlug' details finished"
  }
}

object Endpoint {

  def apply[F[_]: Parallel: Async: GitLabClient: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[Endpoint[F]] =
    for {
      config                <- Async[F].blocking(ConfigFactory.load())
      projectFinder         <- ProjectFinder[F]
      jsonEncoder           <- ProjectJsonEncoder[F]
      tgClient              <- triplesgenerator.api.events.Client[F]
      executionTimeRecorder <- ExecutionTimeRecorderLoader[F](config)
      gitLabUrl             <- GitLabClientLoader.gitLabUrl[F](config)
    } yield new EndpointImpl[F](projectFinder,
                                jsonEncoder,
                                ProjectJsonLDEncoder,
                                tgClient,
                                executionTimeRecorder,
                                gitLabUrl
    )

  def href(renkuApiUrl: renku.ApiUrl, projectSlug: projects.Slug): Href =
    Href(renkuApiUrl / "projects" / projectSlug)
}
