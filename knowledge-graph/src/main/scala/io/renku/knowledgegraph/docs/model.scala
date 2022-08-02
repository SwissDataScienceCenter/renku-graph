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

import cats.Show
import cats.syntax.all._
import io.circe.{Encoder, Json}
import io.renku.knowledgegraph.docs.model.Example.JsonExample
import io.renku.knowledgegraph.docs.model.OAuthFlows.OAuthFlow
import io.renku.knowledgegraph.docs.model.Path.OpMapping

object model {

  trait OpenApiDocument {
    def openApiVersion: String
    def info:           Info
    def servers:        List[Server]
    def paths:          Map[String, Path]
    def components:     Option[Components]
    def security:       List[SecurityRequirement]
  }

  object OpenApiDocument {
    def apply(openApiVersion: String, info: Info): DocWithInfo =
      DocWithInfo(openApiVersion, info, Nil, Map.empty)
  }

  private[model] case class DocWithInfo(openApiVersion: String,
                                        info:           Info,
                                        servers:        List[Server],
                                        paths:          Map[String, Path]
  ) {

    def addPath(path: Path): CompleteDoc =
      CompleteDoc(openApiVersion, info, servers, paths + (path.template -> path), None, Nil)

    def addServer(server: Server): DocWithInfo = copy(openApiVersion, info, servers :+ server, paths)
  }

  private case class CompleteDoc(openApiVersion: String,
                                 info:           Info,
                                 servers:        List[Server],
                                 paths:          Map[String, Path],
                                 components:     Option[Components],
                                 security:       List[SecurityRequirement]
  ) extends OpenApiDocument {

    def addPath(path: Path): CompleteDoc = copy(paths = paths + (path.template -> path))

    def addServer(server: Server): CompleteDoc = copy(openApiVersion, info, servers :+ server, paths)

    def addNoAuthSecurity(): CompleteDoc =
      copy(security = security :+ SecurityRequirementNoAuth)

    def addSecurity(securityScheme: SecurityScheme) = {
      val newComponents = {
        val c = this.components.getOrElse(Components.empty)
        c.copy(securitySchemes = c.securitySchemes + (securityScheme.name -> securityScheme))
      }
      copy(security = security :+ SecurityRequirementAuth(securityScheme.name, Nil), components = newComponents.some)
    }
  }

  case class Info(title: String, description: Option[String], version: String)

  case class Server(url: String, description: String, variables: Map[String, Variable] = Map.empty)

  case class Variable(default: String)

  object Uri {
    def /(nextPart: Parameter): Uri = Uri(List(ParameterPart(nextPart)))
    def /(nextPart: String):    Uri = Uri(List(StringPart(nextPart)))
    def getTemplate(parts: List[UriPart]): String =
      parts
        .map {
          case ParameterPart(parameter) => s"{${parameter.name}}"
          case StringPart(value)        => value
        }
        .mkString("/")
        .prepended("/")
        .mkString("")

  }

  private[model] case class Uri(parts: List[UriPart]) {
    def show:                   Show[Uri] = Show.show(_ => parts.map(_.show).mkString("/"))
    def /(nextPart: Parameter): Uri       = copy(parts :+ ParameterPart(nextPart))
    def /(nextPart: String):    Uri       = copy(parts :+ StringPart(nextPart))
  }

  private[model] case class UriOp(operation: Operation, parts: List[UriPart]) {
    def show: Show[UriOp] = Show.show(_ => parts.map(_.show).mkString("/"))
    def /(nextPart: Parameter) = copy(parts = parts :+ ParameterPart(nextPart))
    def /(nextPart: String)    = copy(parts = parts :+ StringPart(nextPart))
  }

  trait UriPart {
    val show: Show[UriPart]
  }

  case class StringPart(value: String) extends UriPart {
    override val show: Show[UriPart] = Show.show(_ => value)
  }

  case class ParameterPart(parameter: Parameter) extends UriPart {
    override val show: Show[UriPart] = Show.show(_ => s"{${parameter.name}}")
  }

  trait Path {
    def summary:     String
    def description: Option[String]
    def operations:  List[Operation]
    def parameters:  List[Parameter]
    def template:    String
  }

  object Path {

    def apply(summary: String, description: Option[String] = None, opMapping: OpMapping): Path =
      PathImpl(summary, description, List(opMapping.operation), opMapping.operation.parameters, opMapping.template)

    def apply(summary: String, description: String): PathWithDescriptors =
      PathWithDescriptors(summary, description.some)

    case class OpMapping(template: String, operation: Operation)
  }

  private[model] case class PathWithDescriptors(summary: String, description: Option[String]) {

    def addSingleOperation(uriOp: UriOp): PathImpl =
      PathImpl(summary, description, List(uriOp.operation), getParameters(uriOp.parts), Uri.getTemplate(uriOp.parts))

    def addUri(uri: Uri): PathWithUri =
      PathWithUri(summary, description, getParameters(uri.parts), Uri.getTemplate(uri.parts))

    private def getParameters(parts: List[UriPart]) = parts.flatMap {
      case ParameterPart(parameter) => Some(parameter)
      case _                        => None
    }
  }

  private case class PathWithUri(summary:     String,
                                 description: Option[String],
                                 parameters:  List[Parameter],
                                 template:    String
  ) {
    def addGet(operation: Operation.Get): PathImpl =
      PathImpl(summary, description, List(operation), parameters, template)
  }

  private case class PathImpl(summary:     String,
                              description: Option[String],
                              operations:  List[Operation],
                              parameters:  List[Parameter],
                              template:    String
  ) extends Path {
    def addGet(operation: Operation.Get): PathImpl =
      PathImpl(summary, description, operations :+ operation, parameters, template)
  }

  sealed trait Operation extends Product with Serializable {
    def summary:     Option[String]
    def parameters:  List[Parameter]
    def requestBody: Option[RequestBody]
    def responses:   Map[Status, Response]
    def security:    List[SecurityRequirement]
  }

  object Operation {

    def GET(uri: Uri, statusAndResponse: (Status, Response)*): OpMapping = {
      val template = Uri.getTemplate(uri.parts)
      val parameters = uri.parts.flatMap {
        case ParameterPart(parameter) => Some(parameter)
        case _                        => None
      }

      OpMapping(template, Get("".some, parameters, None, statusAndResponse.toMap, Nil))
    }

    case class Get(summary:     Option[String],
                   parameters:  List[Parameter],
                   requestBody: Option[RequestBody],
                   responses:   Map[Status, Response],
                   security:    List[SecurityRequirement]
    ) extends Operation
  }

  case class Parameter(name: String, in: In, description: Option[String], required: Boolean, schema: Schema)

  object Parameter {
    def in(name: String, schema: Schema, description: Option[String] = None, required: Boolean = true): Parameter =
      Parameter(name, In.Path, description, required, schema)
  }

  sealed trait In extends Product with Serializable { val value: String }
  object In {

    final case object Query extends In {
      override val value: String = "query"
    }

    final case object Header extends In {
      override val value: String = "header"
    }

    final case object Path extends In {
      override val value: String = "path"
    }

    final case object Cookie extends In {
      override val value: String = "cookie"
    }
  }

  final case class RequestBody(description: String, content: Map[String, MediaType])

  final case class MediaType(name: String, examples: Map[String, Example])
  object MediaType {

    def apply(name: String, exampleName: String, example: Example): MediaType =
      MediaType(name, Map(exampleName -> example))

    def `application/json`(exampleName: String, example: Json): MediaType =
      MediaType("application/json", Map(exampleName -> JsonExample(example)))

    def `application/json`[P](exampleName: String, example: P)(implicit encoder: Encoder[P]): MediaType =
      MediaType("application/json", Map(exampleName -> JsonExample(encoder(example))))
  }

  final case class Response(
      description: String,
      content:     Map[String, MediaType] = Map.empty,
      headers:     Map[String, Header] = Map.empty,
      links:       Map[String, Link] = Map.empty
  )

  object Contents {
    def apply(mediaTypes: MediaType*): Map[String, MediaType] =
      mediaTypes.map(media => media.name -> media).toMap
  }

  class Status(val code: Int, val name: String)
  object Status {

    def apply(code: Int, name: String): Status = new Status(code, name)

    case object Ok extends Status(200, "Ok")

    case object BadRequest   extends Status(400, "Bad Request")
    case object Unauthorized extends Status(401, "Unauthorized")
    case object NotFound     extends Status(404, "Not Found")

    case object InternalServerError extends Status(500, "Internal Server Error")
  }

  trait SecurityRequirement
  final case class SecurityRequirementAuth(schemeName: String, scopeNames: List[String]) extends SecurityRequirement
  final object SecurityRequirementNoAuth                                                 extends SecurityRequirement

  final case class SecurityScheme(name: String, `type`: TokenType, description: Option[String], in: In)
  object SecurityScheme {

    sealed trait SchemeType { val value: String }
    object SchemeType {
      final case object BearerToken extends SchemeType {
        override val value: String = "Bearer"
      }
      final case object BasicToken extends SchemeType {
        override val value: String = "Basic"
      }
      final case object OAuth2Token extends SchemeType {
        override val value: String = "OAuth"
      }
    }
  }

  final case class Header(description: Option[String], schema: Schema)

  final case class Link()

  sealed trait TokenType extends Product with Serializable {
    def value: String
  }
  object TokenType {
    case object ApiKey extends TokenType {
      override def value: String = "apiKey"
    }
    case object OAuth2 extends TokenType {
      override def value: String = "oauth2"
    }
    case object OpenIdConnect extends TokenType {
      override def value: String = "openIdConnect"
    }
    case object Http extends TokenType {
      override def value: String = "http"
    }
  }

  trait OAuthFlows {
    def `implicit`:        Option[OAuthFlow]
    def password:          Option[OAuthFlow]
    def clientCredentials: Option[OAuthFlow]
    def authorizationCode: Option[OAuthFlow]
  }
  private[model] final case class OAuthFlowsImpl(`implicit`:        Option[OAuthFlow] = None,
                                                 password:          Option[OAuthFlow] = None,
                                                 clientCredentials: Option[OAuthFlow] = None,
                                                 authorizationCode: Option[OAuthFlow] = None
  ) extends OAuthFlows

  object OAuthFlows {
    import OAuthFlowType._

    def apply(flow: OAuthFlow): OAuthFlows = flow.`type` match {
      case Implicit          => OAuthFlowsImpl(Some(flow), None, None, None)
      case Password          => OAuthFlowsImpl(None, Some(flow), None, None)
      case ClientCredentials => OAuthFlowsImpl(None, None, Some(flow), None)
      case AuthorizationCode => OAuthFlowsImpl(None, None, None, Some(flow))
    }

    final case class OAuthFlow(
        `type`:           OAuthFlowType,
        authorizationUrl: String,
        tokenUrl:         String,
        scopes:           Map[String, String],
        refreshUrl:       Option[String] = None
    )

    sealed trait OAuthFlowType { val value: String }

    object OAuthFlowType {
      final case object Implicit extends OAuthFlowType {
        override val value: String = "implicit"
      }

      final case object Password extends OAuthFlowType {
        override val value: String = "password"
      }

      final case object ClientCredentials extends OAuthFlowType {
        override val value: String = "clientCredentials"
      }

      final case object AuthorizationCode extends OAuthFlowType {
        override val value: String = "authorizationCode"
      }
    }
  }

  sealed trait Schema {
    def `type`: String
  }
  object Schema {

    final case object String extends Schema {
      val `type`: String = "string"
    }

    final case object Integer extends Schema {
      val `type`: String = "integer"
    }
  }

  final case class Components(schemas:         Map[String, Schema],
                              examples:        Map[String, Example],
                              securitySchemes: Map[String, SecurityScheme]
  )

  object Components {
    def empty: Components = Components(Map.empty, Map.empty, Map.empty)
  }

  trait Example {
    def value:   T
    def summary: Option[String]
    type T
  }

  object Example {

    case class JsonExample(value: Json, summary: Option[String] = None) extends Example {
      type T = Json
    }

    case class StringExample(value: String, summary: Option[String] = None) extends Example {
      type T = String
    }
  }
}
