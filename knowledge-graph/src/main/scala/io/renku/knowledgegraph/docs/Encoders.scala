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

import io.circe.generic.semiauto.deriveEncoder
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json, JsonObject}
import io.renku.knowledgegraph.docs.model.Example.{JsonExample, StringExample}
import io.renku.knowledgegraph.docs.model._

object Encoders {

  private val empty = Json.obj()
  implicit val docEncoder: Encoder[OpenApiDocument] = Encoder.instance { doc =>
    val components: Json = doc.components.map(c => json"""{"components": $c}""").getOrElse(JsonObject.empty.asJson)
    json"""{
      "openapi":  ${doc.openApiVersion},
      "info":     ${doc.info},
      "servers":  ${doc.servers},
      "paths":    ${doc.paths},
      "security": ${doc.security}
    }""" deepMerge components
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
      "summary": ${path.summary}
    }""" deepMerge description deepMerge operations
  }
  implicit val operationEncoder: Encoder[model.Operation] = Encoder.instance { operation =>
    val summary     = operation.summary.map(s => json"""{"summary": $s }""").getOrElse(empty)
    val requestBody = operation.requestBody.map(r => json"""{"requestBody": $r }""").getOrElse(empty)
    val responses = operation.responses
      .map { case (status, response) =>
        json"""{
          ${status.code}: {
            "description": ${response.description},
            "content":     ${response.content},
            "links":       ${response.links},
            "headers":     ${response.headers}
          } 
        }"""
      }
      .foldLeft(empty)((acc, opJson) => acc deepMerge opJson)
    json"""{
      "security": ${operation.security},
      "parameters": ${operation.parameters},
      "responses": $responses
    }""" deepMerge summary deepMerge requestBody
  }
  implicit val parameterEncoder: Encoder[model.Parameter] = deriveEncoder
  implicit val schemaEncoder: Encoder[model.Schema] = Encoder.instance { schema =>
    json"""{"type": ${schema.`type`}}"""
  }
  implicit val requestBodyEncoder: Encoder[model.RequestBody] = Encoder.instance { requestBody =>
    val content = requestBody.content.foldLeft(empty) { case (acc, (key, mediaType)) =>
      json"""{$key: $mediaType}""" deepMerge acc
    }
    json"""{
      "description": ${requestBody.description},
      "content": $content
    }""" deepMerge content
  }
  implicit val mediaTypeEncoder: Encoder[model.MediaType] = Encoder.instance { mediaType =>
    json"""{
      "examples": ${mediaType.examples}
    }"""
  }
  implicit val responseEncoder: Encoder[model.Response] = Encoder.instance { response =>
    json"""{
      "description": ${response.description}, 
      "content":     ${response.content}
    }"""
  }
  implicit val statusEncoder: Encoder[model.Status] = deriveEncoder
  implicit val headerEncoder: Encoder[model.Header] = deriveEncoder
  implicit val linkEncoder:   Encoder[model.Link]   = deriveEncoder
  implicit lazy val securityRequirementEncoder: Encoder[model.SecurityRequirement] = Encoder.instance {
    case SecurityRequirementAuth(name, scopes) =>
      json"""{$name: $scopes}"""
    case SecurityRequirementNoAuth =>
      json"""{}"""
  }
  implicit val inEncoder: Encoder[model.In] = Encoder.instance { inType =>
    Json.fromString(inType.value)
  }
  implicit val oAuthFlowsEncoder: Encoder[model.OAuthFlows] = Encoder.instance { oAuthFlows =>
    val implicitFlow = oAuthFlows.`implicit`.map(i => json"""{"implicit": $i}""").getOrElse(empty)
    val passwordFlow = oAuthFlows.password.map(p => json"""{"password": $p}""").getOrElse(empty)
    val clientCredentialsFlow =
      oAuthFlows.clientCredentials.map(c => json"""{"clientCredentials": $c}""").getOrElse(empty)
    val authorizationCodeFlow =
      oAuthFlows.authorizationCode.map(a => json"""{"authorizationCode": $a}""").getOrElse(empty)
    empty deepMerge implicitFlow deepMerge passwordFlow deepMerge clientCredentialsFlow deepMerge authorizationCodeFlow
  }
  implicit val oAuthFlowEncoder: Encoder[model.OAuthFlows.OAuthFlow] = Encoder.instance { flow =>
    val refreshUrl = flow.refreshUrl.map(s => json"""{"refreshUrl": $s }""").getOrElse(empty)
    json"""{
      "authorizationUrl": ${flow.authorizationUrl},
      "tokenUrl": ${flow.tokenUrl},
      "scopes": ${flow.scopes}
    }""" deepMerge refreshUrl
  }
  implicit val componentsEncoder: Encoder[model.Components] = Encoder.instance { components =>
    json"""{
      "schemas": ${components.schemas},
      "examples": ${components.examples},
      "securitySchemes": ${components.securitySchemes}
    }"""
  }
  implicit val securitySchemeEncoder: Encoder[model.SecurityScheme] = Encoder.instance { scheme =>
    val description = scheme.description.map(s => json"""{"description": $s }""").getOrElse(empty)
    json"""{
      "name": ${scheme.name},
      "type": ${scheme.`type`.value},
      "in": ${scheme.in}
    }""" deepMerge description
  }
  implicit def exampleEncoder: Encoder[model.Example] = Encoder.instance { example =>
    val value = example match {
      case JsonExample(value, _)   => json"""{"value": $value}"""
      case StringExample(value, _) => json"""{"value": $value}"""

    }
    val summary = example.summary.map(s => json"""{"summary": $s}""").getOrElse(empty)

    value deepMerge summary
  }
}
