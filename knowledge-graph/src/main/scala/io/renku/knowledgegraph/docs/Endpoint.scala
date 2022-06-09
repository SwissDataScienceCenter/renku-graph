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
import io.circe._
import io.circe.generic.semiauto._
import io.circe.literal.JsonStringContext
import io.circe.syntax._
import io.renku.knowledgegraph.docs.model.Example.{JsonExample, StringExample}
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
  import Endpoint._

  override def `get /docs`: F[Response[F]] = Ok(doc.asJson)

  lazy val doc: OpenApiDocument =
    OpenApiDocument(
      "3.0.3",
      Info("Knowledge Graph API", "Get info about datasets, users, activities, and other entities".some, "1.0.0")
    ).addServer(localServer).addPath(lineage.EndpointDoc.path)

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
    "???",
    model.OAuthFlows(`implicit` =
      OAuthFlow(
        authorizationUrl = "https://dev.renku.ch/auth/realms/Renku/protocol/openid-connect/auth",
        tokenUrl = "https://dev.renku.ch/auth/realms/Renku/protocol/openid-connect/token",
        scopes = Map.empty
      ).some
    ),
    openIdConnectUrl = "/auth/realms/Renku/.well-known/openid-configuration"
  )

}
private object Endpoint {
  private val empty = Json.obj()
  implicit val docEncoder: Encoder[OpenApiDocument] = Encoder.instance { doc =>
    val components: Json = doc.maybeComponents.map(c => json"""{"components": $c}""").getOrElse(JsonObject.empty.asJson)
    json"""
          {
            "openapi": ${doc.openApiVersion},
            "info": ${doc.info},
            "servers": ${doc.servers},
            "paths": ${doc.paths}
          }
        """ deepMerge components
  }
  implicit val infoEncoder:     Encoder[Info]           = deriveEncoder
  implicit val serverEncoder:   Encoder[Server]         = deriveEncoder
  implicit val variableEncoder: Encoder[model.Variable] = deriveEncoder
  implicit val pathEncoder: Encoder[model.Path] = Encoder.instance { path =>
    val description = path.description.map(s => json"""{"description": $s }""").getOrElse(empty)

    val operations: Json = path.operations
      .map { operation: Operation =>
        operation match {
          case _: Operation.Get =>
            json"""{"get": $operation}"""
        }
      }
      .foldLeft(json"""{}""")((acc, opJson) => acc deepMerge opJson)

    json"""{
          "parameters": ${path.parameters},
          "summary": ${path.summary}}
          """ deepMerge description deepMerge operations
  }
  implicit val operationEncoder: Encoder[model.Operation] = Encoder.instance { operation =>
    val summary     = operation.summary.map(s => json"""{"summary": $s }""").getOrElse(empty)
    val requestBody = operation.requestBody.map(r => json"""{"requestBody": $r }""").getOrElse(empty)
    val responses = operation.responses
      .map { case (status, response) =>
        json"""{
               ${status.code}: {
                    "description": ${response.description},
                    "content": ${response.content},
                    "content": ${response.links},
                    "content": ${response.headers}
               } 
             }"""
      }
      .foldLeft(empty)((acc, opJson) => acc deepMerge opJson)
    json"""
           {
             "security": ${operation.security},
             "parameters": ${operation.parameters},
             "responses": $responses
           }
           """ deepMerge summary deepMerge requestBody
  }
  implicit val parameterEncoder: Encoder[model.Parameter] = deriveEncoder
  implicit val schemaEncoder: Encoder[model.Schema] = Encoder.instance { schema =>
    json"""{"type": ${schema.`type`}}"""
  }
  implicit val requestBodyEncoder: Encoder[model.RequestBody] = deriveEncoder
  implicit val mediaTypeEncoder:   Encoder[model.MediaType]   = deriveEncoder
  implicit val responseEncoder: Encoder[model.Response] = Encoder.instance { response =>
    json"""{
             "description": ${response.description}, 
             "content": ${response.content}
           }"""
  }
  implicit val statusEncoder:              Encoder[model.Status]              = deriveEncoder
  implicit val headerEncoder:              Encoder[model.Header]              = deriveEncoder
  implicit val linkEncoder:                Encoder[model.Link]                = deriveEncoder
  implicit val securityRequirementEncoder: Encoder[model.SecurityRequirement] = deriveEncoder
  implicit val inEncoder: Encoder[model.In] = Encoder.instance { inType =>
    Json.fromString(inType.value)
  }
  implicit val componentsEncoder: Encoder[model.Components] = deriveEncoder
  implicit def exampleEncoder: Encoder[model.Example] = Encoder.instance { example =>
    val value = example match {
      case JsonExample(value, _)   => json"""{"value": $value }"""
      case StringExample(value, _) => json"""{"value": $value }"""

    }
    val summary = example.summary.map(s => json"""{"summary": $s }""").getOrElse(empty)

    value deepMerge summary
  }

  def apply[F[_]: Async]: F[Endpoint[F]] = new EndpointImpl[F].pure[F].widen[Endpoint[F]]
}
