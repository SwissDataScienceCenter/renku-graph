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
import cats.implicits.catsSyntaxApplicativeId
import cats.syntax.all._
import io.circe.syntax._
import io.renku.knowledgegraph.docs.model.OAuthFlows.OAuthFlow
import io.renku.knowledgegraph.docs.model.SecurityScheme.OAuth2Token
import io.renku.knowledgegraph.docs.model._
import io.renku.knowledgegraph.lineage
import org.http4s.Response
import org.http4s.circe.jsonEncoder
import org.http4s.dsl.Http4sDsl

trait Endpoint[F[_]] {
  def `get /docs`: F[Response[F]]
}
// TODO: add authorization info: https://github.com/SwissDataScienceCenter/renku-graph/tree/development/knowledge-graph#get-knowledge-graphprojectsnamespacename
// specify different responses: e.g. 401, 403, 404, 500, etc.
private class EndpointImpl[F[_]: Async] extends Http4sDsl[F] with Endpoint[F] {
  import Encoders._

  override def `get /docs`: F[Response[F]] = Ok(doc.asJson)

  lazy val doc: OpenApiDocument =
    OpenApiDocument(
      "3.0.3",
      Info("Knowledge Graph API", "Get info about datasets, users, activities, and other entities".some, "1.0.0")
    ).addServer(localServer)
      .addPath(lineage.EndpointDoc.path)
      .addSecurity(securityScheme)

  private lazy val localServer =
    Server("http://localhost:{port}/{basePath}",
           "Local server",
           Map("port" -> Variable("8080"), "basePath" -> Variable("knowledge-graph"))
    )

  private lazy val securityScheme = SecurityScheme(
    "PRIVATE-TOKEN",
    TokenType.ApiKey,
    "User's Personal Access Token in GitLab".some,
    In.Header,
    OAuth2Token,
    model.OAuthFlows(
      OAuthFlow(
        `type` = model.OAuthFlows.OAuthFlowType.Password,
        authorizationUrl = "https://dev.renku.ch/auth/realms/Renku/protocol/openid-connect/auth",
        tokenUrl = "https://dev.renku.ch/auth/realms/Renku/protocol/openid-connect/token",
        scopes = Map.empty
      )
    ),
    openIdConnectUrl = "/auth/realms/Renku/.well-known/openid-configuration"
  )

}
object Endpoint {

  def apply[F[_]: Async]: F[Endpoint[F]] = new EndpointImpl[F].pure[F].widen[Endpoint[F]]
}
