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

package io.renku.triplesgenerator.api

import cats.effect.Async
import cats.syntax.all._
import io.renku.control.Throttler
import io.renku.graph.config.TriplesGeneratorUrl
import io.renku.graph.model.projects
import io.renku.http.client.RestClient
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.api.Client.Result
import org.http4s.Uri
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait Client[F[_]] {
  def updateProject(slug: projects.Slug, updates: ProjectUpdates): F[Result[Unit]]
}

object Client {

  def apply[F[_]: Async: Logger: MetricsRegistry]: F[Client[F]] =
    TriplesGeneratorUrl[F]()
      .map(tgUrl => new ClientImpl[F](Uri.unsafeFromString(tgUrl.value)))

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

private class ClientImpl[F[_]: Async: Logger](tgUri: Uri)
    extends RestClient[F, Nothing](Throttler.noThrottling)
    with Client[F]
    with Http4sDsl[F]
    with Http4sClientDsl[F] {

  import io.circe.syntax._
  import io.renku.http.tinytypes.TinyTypeURIEncoder._
  import org.http4s.circe._

  override def updateProject(slug: projects.Slug, updates: ProjectUpdates): F[Result[Unit]] =
    send(PUT(tgUri / "projects" / slug) withEntity updates.asJson) {
      case (Ok, _, _)       => Result.success(()).pure[F]
      case (NotFound, _, _) => Result.failure[Unit]("Project for update does not exist").pure[F]
      case (status, req, _) =>
        Result.failure[Unit](s"Updating project in TG failed: ${req.pathInfo.renderString}: $status").pure[F]
    }
}
