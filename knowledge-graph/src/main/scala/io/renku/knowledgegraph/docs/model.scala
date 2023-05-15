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

import cats.Show
import cats.syntax.all._
import io.circe.{Encoder, Json}
import io.renku.jsonld.JsonLD
import io.renku.knowledgegraph.docs.model.Example.{JsonExample, JsonLDExample}
import io.renku.knowledgegraph.docs.model.OAuthFlows.OAuthFlow
import io.renku.knowledgegraph.docs.model.Path.OpMapping

object model {

  trait OpenApiDocument {
    def openApiVersion: String
    def info:           Info
    def servers:        List[Server]
    def paths:          List[(String, Path)]
    def components:     Option[Components]
    def security:       List[SecurityRequirement]
  }

  object OpenApiDocument {
    def apply(openApiVersion: String, info: Info): DocWithInfo =
      DocWithInfo(openApiVersion, info, servers = Nil, paths = List.empty)
  }

  private[model] case class DocWithInfo(openApiVersion: String,
                                        info:           Info,
                                        servers:        List[Server],
                                        paths:          List[(String, Path)]
  ) {

    def addPath(path: Path): CompleteDoc =
      CompleteDoc(openApiVersion, info, servers, (path.template -> path) :: paths, None, Nil)

    def addServer(server: Server): CompleteDoc =
      CompleteDoc(openApiVersion, info, server :: servers, paths, None, Nil)
  }

  private case class CompleteDoc(openApiVersion: String,
                                 info:           Info,
                                 servers:        List[Server],
                                 paths:          List[(String, Path)],
                                 components:     Option[Components],
                                 security:       List[SecurityRequirement]
  ) extends OpenApiDocument {

    def addPath(path: Path): CompleteDoc =
      copy(paths = path.template -> path :: paths)

    def addServer(server: Server): CompleteDoc =
      copy(openApiVersion, info, server :: servers, paths)

    def addNoAuthSecurity(): CompleteDoc =
      copy(security = SecurityRequirementNoAuth :: security)

    def addSecurity(securityScheme: SecurityScheme): CompleteDoc = copy(
      security = SecurityRequirementAuth(securityScheme.id, Nil) :: security,
      components = {
        val c = components.getOrElse(Components.empty)
        c.copy(securitySchemes = c.securitySchemes + (securityScheme.id -> securityScheme))
      }.some
    )
  }

  case class Info(title: String, description: Option[String], version: String)

  case class Server(url: String, description: String, variables: Map[String, Variable] = Map.empty)

  case class Variable(default: String)

  object Uri {

    def /(nextPart: Parameter): Uri = Uri(List(ParameterPart(nextPart)))
    def /(nextPart: String):    Uri = Uri(List(StringPart(nextPart)))

    def getTemplate(parts: List[UriPart]): String =
      parts
        .collect {
          case ParameterPart(parameter: Parameter.Path) => s"{${parameter.name}}"
          case StringPart(value)                        => value
        }
        .mkString("/")
        .prepended('/')
        .mkString("")

  }

  private[model] case class Uri(parts: List[UriPart]) {
    def show: Show[Uri] = Show.show(_ => parts.map(_.show).mkString("/"))
    def /(nextPart:  Parameter.Path):  Uri = copy(parts :+ ParameterPart(nextPart))
    def /(nextPart:  String):          Uri = copy(parts :+ StringPart(nextPart))
    def :?(nextPart: Parameter.Query): Uri = copy(parts :+ ParameterPart(nextPart))
    def &(nextPart:  Parameter.Query): Uri = copy(parts :+ ParameterPart(nextPart))
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

    case class OpMapping(template: String, operation: Operation)
  }

  private case class PathImpl(summary:     String,
                              description: Option[String],
                              operations:  List[Operation],
                              parameters:  List[Parameter],
                              template:    String
  ) extends Path

  sealed trait Operation extends Product with Serializable {
    def summary:     Option[String]
    def parameters:  List[Parameter]
    def requestBody: Option[RequestBody]
    def responses:   Map[Status, Response]
    def security:    List[SecurityRequirement]
  }

  object Operation {

    def GET(uri: Uri, statusAndResponse: (Status, Response)*): OpMapping = {
      val parameters = uri.parts.flatMap {
        case ParameterPart(parameter) => Some(parameter)
        case _                        => None
      }

      OpMapping(Uri.getTemplate(uri.parts), Get("".some, parameters, None, statusAndResponse.toMap, Nil))
    }

    def DELETE(uri: Uri, statusAndResponse: (Status, Response)*): OpMapping = {
      val parameters = uri.parts.flatMap {
        case ParameterPart(parameter) => Some(parameter)
        case _                        => None
      }

      OpMapping(Uri.getTemplate(uri.parts), Delete("".some, parameters, None, statusAndResponse.toMap, Nil))
    }

    case class Get(summary:     Option[String],
                   parameters:  List[Parameter],
                   requestBody: Option[RequestBody],
                   responses:   Map[Status, Response],
                   security:    List[SecurityRequirement]
    ) extends Operation

    case class Delete(summary:     Option[String],
                      parameters:  List[Parameter],
                      requestBody: Option[RequestBody],
                      responses:   Map[Status, Response],
                      security:    List[SecurityRequirement]
    ) extends Operation
  }

  sealed trait Parameter {
    type InType <: In
    val name:        String
    val in:          InType
    val description: Option[String]
    val required:    Boolean
    val schema:      Schema
  }
  object Parameter {

    final case class Path(name: String, schema: Schema, description: Option[String] = None, required: Boolean = true)
        extends Parameter {
      override type InType = In.Path.type
      override val in: InType = In.Path
    }
    final case class Query(name: String, schema: Schema, description: Option[String] = None, required: Boolean = true)
        extends Parameter {
      override type InType = In.Query.type
      override val in: InType = In.Query
    }
    final case class Header(name: String, schema: Schema, description: Option[String] = None, required: Boolean = true)
        extends Parameter {
      override type InType = In.Header.type
      override val in: InType = In.Header
    }
    final case class Cookie(name: String, schema: Schema, description: Option[String] = None, required: Boolean = true)
        extends Parameter {
      override type InType = In.Cookie.type
      override val in: InType = In.Cookie
    }
  }

  sealed trait In extends Product with Serializable { val value: String }
  object In {

    final case object Path extends In {
      override val value: String = "path"
    }

    final case object Query extends In {
      override val value: String = "query"
    }

    final case object Header extends In {
      override val value: String = "header"
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

    lazy val `text/html`: MediaType = MediaType("text/html", Map.empty)

    def `application/json`(exampleName: String, example: Json): MediaType =
      MediaType("application/json", Map(exampleName -> JsonExample(example)))

    def `application/ld+json`(exampleName: String, example: JsonLD): MediaType =
      MediaType("application/ld+json", Map(exampleName -> JsonLDExample(example)))

    def `application/json`[P](exampleName: String, example: P)(implicit encoder: Encoder[P]): MediaType =
      MediaType("application/json", Map(exampleName -> JsonExample(encoder(example))))

    def `application/json`: MediaType =
      MediaType("application/json", Map.empty)
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

    case object Ok       extends Status(200, "Ok")
    case object Created  extends Status(201, "Created")
    case object Accepted extends Status(202, "Accepted")

    case object BadRequest   extends Status(400, "Bad Request")
    case object Unauthorized extends Status(401, "Unauthorized")
    case object NotFound     extends Status(404, "Not Found")

    case object InternalServerError extends Status(500, "Internal Server Error")
  }

  trait SecurityRequirement
  final case class SecurityRequirementAuth(schemeName: String, scopeNames: List[String]) extends SecurityRequirement
  final object SecurityRequirementNoAuth                                                 extends SecurityRequirement

  sealed trait SecurityScheme {
    val id:          String
    val `type`:      TokenType
    val description: Option[String]
  }
  object SecurityScheme {
    final case class ApiKey(id: String, name: String, description: Option[String] = None, in: In = In.Header)
        extends SecurityScheme {
      override val `type`: TokenType = TokenType.ApiKey
    }
    final case class OpenIdConnect(id:               String,
                                   name:             String,
                                   openIdConnectUrl: String,
                                   description:      Option[String] = None,
                                   in:               In = In.Header
    ) extends SecurityScheme {
      override val `type`: TokenType = TokenType.OpenIdConnect
    }
  }

  final case class Header(description: Option[String], schema: Schema)

  final case class Link()

  sealed trait TokenType extends Product with Serializable { val value: String }
  object TokenType {
    case object ApiKey extends TokenType {
      override val value: String = "apiKey"
    }
    case object OpenIdConnect extends TokenType {
      override val value: String = "openIdConnect"
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

  sealed trait Schema { val `type`: String }
  object Schema {

    final case object String extends Schema {
      override val `type`: String = "string"
    }

    final case object Integer extends Schema {
      override val `type`: String = "integer"
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
    case class JsonLDExample(value: JsonLD, summary: Option[String] = None) extends Example {
      type T = JsonLD
    }
    case class StringExample(value: String, summary: Option[String] = None) extends Example {
      type T = String
    }
  }
}
