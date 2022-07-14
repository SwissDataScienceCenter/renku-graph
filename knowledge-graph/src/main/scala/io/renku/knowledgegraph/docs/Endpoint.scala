/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.docs

import cats.effect.Async
import cats.syntax.all._
import io.circe.syntax._
import io.renku.config.ServiceVersion
import io.renku.knowledgegraph.docs.Implicits.StatusOps
import io.renku.knowledgegraph.docs.model._
import io.renku.knowledgegraph.lineage
import org.http4s
import org.http4s.circe.jsonEncoder
import org.http4s.dsl.Http4sDsl

trait Endpoint[F[_]] {
  def `get /docs`: F[http4s.Response[F]]
}

private class EndpointImpl[F[_]: Async](serviceVersion: ServiceVersion) extends Http4sDsl[F] with Endpoint[F] {
  import Encoders._

  override def `get /docs`: F[http4s.Response[F]] = Ok(doc.asJson)

  lazy val doc: OpenApiDocument =
    OpenApiDocument(
      openApiVersion = "3.0.3",
      Info("Knowledge Graph API",
           "Get info about datasets, users, activities, and other entities".some,
           serviceVersion.value
      )
    ).addServer(renkuLabServer)
      .addPath(lineage.EndpointDoc.path)
      .addSecurity(securityScheme)
      .addResponsesToAll(authErrorResponses)

  private lazy val localServer =
    Server("http://localhost:{port}/{basePath}",
           "Local server",
           Map("port" -> Variable("8080"), "basePath" -> Variable("knowledge-graph/projects"))
    )

  private lazy val devServer =
    Server("http://dev.renku.ch/{basePath}", "Dev server", Map("basePath" -> Variable("knowledge-graph/projects")))

  private lazy val renkuLabServer =
    Server("http://renkulab.io/{basePath}", "Renku Lab server", Map("basePath" -> Variable("knowledge-graph/projects")))

  private lazy val securityScheme = SecurityScheme(
    "PRIVATE-TOKEN",
    TokenType.ApiKey,
    "User's Personal Access Token in GitLab".some,
    In.Header
  )

  val authErrorResponses = Map(
    http4s.Status.Unauthorized.asDocStatus -> Response("If given auth header cannot be authenticated",
                                                       Map.empty,
                                                       Map.empty,
                                                       Map.empty
    ),
    http4s.Status.NotFound.asDocStatus -> Response(
      "If there is no project with the given namespace/name or user is not authorised to access this project",
      Map.empty,
      Map.empty,
      Map.empty
    ),
    http4s.Status.InternalServerError.asDocStatus -> Response("Otherwise", Map.empty, Map.empty, Map.empty)
  )

}
object Endpoint {

  def apply[F[_]: Async]: F[Endpoint[F]] = {
    val serviceVersion: F[ServiceVersion] = ServiceVersion.readFromConfig[F]()
    serviceVersion.map(new EndpointImpl[F](_)).widen[Endpoint[F]]
  }
}
