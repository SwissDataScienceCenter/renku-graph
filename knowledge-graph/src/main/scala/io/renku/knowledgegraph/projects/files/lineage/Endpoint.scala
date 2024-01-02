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

package io.renku.knowledgegraph.projects.files.lineage

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.syntax._
import io.renku.data.Message
import io.renku.graph.model.projects
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.SparqlQueryTimeRecorder
import model.Lineage
import model.Node.Location
import org.http4s.Response
import org.http4s.circe.jsonEncoder
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait Endpoint[F[_]] {
  def `GET /lineage`(projectSlug: projects.Slug, location: Location, maybeUser: Option[AuthUser]): F[Response[F]]
}

private class EndpointImpl[F[_]: Async: Logger](lineageFinder: LineageFinder[F]) extends Http4sDsl[F] with Endpoint[F] {

  override def `GET /lineage`(projectSlug: projects.Slug,
                              location:    Location,
                              maybeUser:   Option[AuthUser]
  ): F[Response[F]] =
    lineageFinder
      .find(projectSlug, location, maybeUser)
      .flatMap(toHttpResult(projectSlug, location))
      .recoverWith(httpResult)

  private def toHttpResult(projectSlug: projects.Slug, location: Location): Option[Lineage] => F[Response[F]] = {
    case None          => NotFound(Message.Info.unsafeApply(show"No lineage for project: $projectSlug file: $location"))
    case Some(lineage) => Ok(lineage.asJson)
  }

  private lazy val httpResult: PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = Message.Error("Lineage generation failed")
    Logger[F].error(exception)(errorMessage.show) >> InternalServerError(errorMessage)
  }
}

object Endpoint {
  def apply[F[_]: Async: Parallel: Logger: SparqlQueryTimeRecorder]: F[Endpoint[F]] =
    LineageFinder[F].map(new EndpointImpl[F](_))
}
