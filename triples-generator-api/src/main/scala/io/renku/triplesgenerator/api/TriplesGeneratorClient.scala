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

package io.renku.triplesgenerator.api

import TriplesGeneratorClient.Result
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.control.Throttler
import io.renku.graph.config.TriplesGeneratorUrl
import io.renku.graph.model.projects
import io.renku.http.client.RestClient
import io.renku.metrics.MetricsRegistry
import org.http4s.Uri
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait TriplesGeneratorClient[F[_]] {
  def createProject(newProject: NewProject): F[Result[Unit]]
  def updateProject(slug:       projects.Slug, updates: ProjectUpdates): F[Result[Unit]]
}

object TriplesGeneratorClient {

  def apply[F[_]: Async: Logger: MetricsRegistry]: F[TriplesGeneratorClient[F]] = apply()

  def apply[F[_]: Async: Logger: MetricsRegistry](config: Config = ConfigFactory.load): F[TriplesGeneratorClient[F]] =
    TriplesGeneratorUrl[F](config)
      .map(tgUrl => new TriplesGeneratorClientImpl[F](Uri.unsafeFromString(tgUrl.value)))

  sealed trait Result[+A] {
    def toEither: Either[Throwable, A]
  }

  object Result {
    final case class Success[+A](value: A) extends Result[A] {
      def toEither: Either[Throwable, A] = Right(value)
    }

    final case class Failure(error: String) extends RuntimeException(error) with Result[Nothing] {
      def toEither: Either[Throwable, Nothing] = Left(this)
    }

    def success[A](value: A): Result[A] = Success(value)

    def failure[A](error: String): Result[A] = Failure(error)
  }
}

private class TriplesGeneratorClientImpl[F[_]: Async: Logger](tgUri: Uri)
    extends RestClient[F, Nothing](Throttler.noThrottling)
    with TriplesGeneratorClient[F]
    with Http4sDsl[F]
    with Http4sClientDsl[F] {

  import io.circe.syntax._
  import org.http4s.circe.CirceEntityCodec._
  import io.renku.http.tinytypes.TinyTypeURIEncoder._

  override def createProject(newProject: NewProject): F[Result[Unit]] =
    send(POST(tgUri / "projects") withEntity newProject.asJson) {
      case (Created, _, _) => Result.success(()).pure[F]
      case (status, req, _) =>
        Result.failure[Unit](s"Creating project in TG failed: ${req.pathInfo.renderString}: $status").pure[F]
    }

  override def updateProject(slug: projects.Slug, updates: ProjectUpdates): F[Result[Unit]] =
    send(PATCH(tgUri / "projects" / slug) withEntity updates.asJson) {
      case (Ok, _, _)       => Result.success(()).pure[F]
      case (NotFound, _, _) => Result.failure[Unit]("Project for update does not exist").pure[F]
      case (status, req, _) =>
        Result.failure[Unit](s"Updating project in TG failed: ${req.pathInfo.renderString}: $status").pure[F]
    }
}
