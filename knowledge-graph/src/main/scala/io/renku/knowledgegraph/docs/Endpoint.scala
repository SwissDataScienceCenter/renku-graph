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

package io.renku.knowledgegraph.docs

import cats.effect.Async
import cats.syntax.all._
import io.circe.syntax._
import io.renku.config.ServiceVersion
import io.renku.knowledgegraph._
import io.renku.knowledgegraph.docs.model._
import org.http4s
import org.http4s.circe.jsonEncoder
import org.http4s.dsl.Http4sDsl

trait Endpoint[F[_]] {
  def `get /spec.json`: F[http4s.Response[F]]
}

object Endpoint {

  def apply[F[_]: Async]: F[Endpoint[F]] = for {
    serviceVersion <- ServiceVersion.readFromConfig[F]()
    endpointsDocs <- List(
                       datasets.EndpointDocs[F],
                       datasets.details.EndpointDocs[F],
                       entities.EndpointDocs[F],
                       ontology.EndpointDocs[F],
                       projects.delete.EndpointDocs.pure[F].widen[docs.EndpointDocs],
                       projects.details.EndpointDocs[F],
                       projects.update.EndpointDocs.pure[F].widen[docs.EndpointDocs],
                       projects.files.lineage.EndpointDocs.pure[F].widen[docs.EndpointDocs],
                       projects.datasets.EndpointDocs[F],
                       projects.datasets.tags.EndpointDocs[F],
                       users.projects.EndpointDocs[F],
                       entities.currentuser.recentlyviewed.EndpointDocs[F],
                       EndpointDocs[F]
                     ).sequence
  } yield new EndpointImpl[F](serviceVersion, endpointsDocs)
}

private class EndpointImpl[F[_]: Async](serviceVersion: ServiceVersion, endpointsDocs: List[EndpointDocs])
    extends Http4sDsl[F]
    with Endpoint[F] {
  import Encoders._

  override def `get /spec.json`: F[http4s.Response[F]] = Ok(doc.asJson)

  lazy val doc: OpenApiDocument = {

    val document = OpenApiDocument(
      openApiVersion = "3.0.3",
      Info("Knowledge Graph APIs", "APIs to fetch and manipulate data in the Triples Store".some, serviceVersion.value)
    ).addServer(server)

    endpointsDocs
      .foldLeft(document) { (doc, endpoint) =>
        doc.addPath(endpoint.path)
      }
      .addSecurity(privateToken)
      .addSecurity(openIdConnect)
      .addNoAuthSecurity()
  }

  private lazy val server = Server(
    url = "/knowledge-graph",
    description = "Renku Knowledge Graph API"
  )

  private lazy val privateToken = SecurityScheme.ApiKey(
    id = "private-token",
    name = "PRIVATE-TOKEN",
    description = "User's Personal Access Token in GitLab".some
  )

  private lazy val openIdConnect = SecurityScheme.OpenIdConnect(
    id = "oauth_auth",
    name = "oauth_auth",
    openIdConnectUrl = "/auth/realms/Renku/.well-known/openid-configuration"
  )
}
