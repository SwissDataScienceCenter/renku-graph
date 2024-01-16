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

package io.renku.knowledgegraph.docs

import io.circe.generic.semiauto.deriveEncoder
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json, JsonObject}
import io.renku.knowledgegraph.docs.model.Example.{JsonExample, JsonLDExample, StringExample}
import io.renku.knowledgegraph.docs.model._

private object Encoders {

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

  implicit val pathsEncoder: Encoder[model.Paths] = Encoder.instance { case Paths(paths) =>
    Json.obj(
      paths.toList.sortBy(_._1).map { case path -> paths =>
        path -> paths.map(_.asJson).reduce(_ deepMerge _)
      }: _*
    )
  }

  implicit val pathEncoder: Encoder[model.Path] = Encoder.instance { path =>
    val operations: Json = path.operations
      .map { operation: Operation =>
        operation match {
          case _: Operation.Delete => json"""{"delete": $operation}"""
          case _: Operation.Get    => json"""{"get": $operation}"""
          case _: Operation.Post   => json"""{"post": $operation}"""
          case _: Operation.Patch  => json"""{"patch": $operation}"""
          case _: Operation.Put    => json"""{"put": $operation}"""
        }
      }
      .foldLeft(Json.obj())((acc, opJson) => acc deepMerge opJson)

    json"""{
      "summary":     ${path.summary},
      "description": ${path.description},
      "parameters":  ${path.parameters}
    }""".deepDropNullValues deepMerge operations
  }

  implicit val operationEncoder: Encoder[model.Operation] = Encoder.instance { operation =>
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
      .foldLeft(Json.obj())((acc, opJson) => acc deepMerge opJson)
    json"""{
      "summary":     ${operation.summary},
      "description": ${operation.description},
      "requestBody": ${operation.requestBody},
      "security":    ${operation.security},
      "parameters":  ${operation.parameters},
      "responses":   $responses
    }""" deepDropNullValues
  }

  implicit val parameterEncoder: Encoder[model.Parameter] = Encoder.instance { param =>
    json"""{
      "name":        ${param.name},
      "in":          ${param.in.value},
      "description": ${param.description.getOrElse("")},
      "required":    ${param.required},
      "schema":      ${param.schema}
    }"""
  }

  implicit val schemaEncoder: Encoder[model.Schema] = Encoder.instance {
    case sch: model.Schema.String.type =>
      json"""{"type": ${sch.`type`}}"""
    case sch: model.Schema.Integer.type =>
      json"""{"type": ${sch.`type`}}"""
    case sch: model.Schema.EnumString =>
      json"""{"type": ${sch.`type`}, "enum": ${sch.values.toList.sorted}}"""
    case sch: model.Schema.`Object` =>
      json"""{"type": ${sch.`type`}, "properties": ${sch.properties}}"""
  }

  implicit val requestBodyEncoder: Encoder[model.RequestBody] = Encoder.instance { requestBody =>
    val content = requestBody.content.foldLeft(Json.obj()) { case (acc, (key, mediaType)) =>
      json"""{$key: $mediaType}""" deepMerge acc
    }

    json"""{
      "description": ${requestBody.description},
      "required":    ${requestBody.required},
      "content":     $content
    }"""
  }

  implicit val mediaTypeEncoder: Encoder[model.MediaType] = Encoder.instance {
    case MediaType.WithoutSchema(_, examples) =>
      val examplesJson = examples.foldLeft(Json.obj()) { case (json, (name, example)) =>
        json deepMerge json"""{$name: {"value": $example}}"""
      }
      json"""{
        "examples": $examplesJson
      }"""
    case MediaType.WithSchema(_, schema, example) =>
      json"""{
        "schema":  $schema,
        "example": $example
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
      Json.obj()
  }

  implicit val inEncoder: Encoder[model.In] = Encoder.instance { inType =>
    Json.fromString(inType.value)
  }

  implicit val oAuthFlowsEncoder: Encoder[model.OAuthFlows] = Encoder.instance { oAuthFlows =>
    val implicitFlow = oAuthFlows.`implicit`.map(i => json"""{"implicit": $i}""").getOrElse(Json.obj())
    val passwordFlow = oAuthFlows.password.map(p => json"""{"password": $p}""").getOrElse(Json.obj())
    val clientCredentialsFlow =
      oAuthFlows.clientCredentials.map(c => json"""{"clientCredentials": $c}""").getOrElse(Json.obj())
    val authorizationCodeFlow =
      oAuthFlows.authorizationCode.map(a => json"""{"authorizationCode": $a}""").getOrElse(Json.obj())
    Json.obj() deepMerge implicitFlow deepMerge passwordFlow deepMerge clientCredentialsFlow deepMerge authorizationCodeFlow
  }

  implicit val oAuthFlowEncoder: Encoder[model.OAuthFlows.OAuthFlow] = Encoder.instance { flow =>
    val refreshUrl = flow.refreshUrl.map(s => json"""{"refreshUrl": $s }""").getOrElse(Json.obj())
    json"""{
      "authorizationUrl": ${flow.authorizationUrl},
      "tokenUrl":         ${flow.tokenUrl},
      "scopes":           ${flow.scopes}
    }""" deepMerge refreshUrl
  }

  implicit val componentsEncoder: Encoder[model.Components] = Encoder.instance { components =>
    json"""{
      "schemas":         ${components.schemas},
      "examples":        ${components.examples},
      "securitySchemes": ${components.securitySchemes}
    }"""
  }

  implicit val securitySchemeEncoder: Encoder[model.SecurityScheme] = Encoder.instance {
    case sch: SecurityScheme.ApiKey =>
      val description = sch.description.map(s => json"""{"description": $s }""").getOrElse(Json.obj())
      json"""{
        "name": ${sch.name},
        "type": ${sch.`type`.value},
        "in":   ${sch.in}
      }""" deepMerge description
    case sch: SecurityScheme.OpenIdConnect =>
      val description = sch.description.map(s => json"""{"description": $s }""").getOrElse(Json.obj())
      json"""{
        "openIdConnectUrl": ${sch.openIdConnectUrl},
        "type":             ${sch.`type`.value}
      }""" deepMerge description
  }

  implicit def exampleEncoder: Encoder[model.Example] = Encoder.instance {
    case JsonExample(value)   => value
    case JsonLDExample(value) => value.toJson
    case StringExample(value) => Json.fromString(value)
  }
}
