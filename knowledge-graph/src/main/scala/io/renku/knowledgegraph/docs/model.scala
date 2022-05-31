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

object model {
  trait OpenApiDocument {
    def openApiVersion: String
    def info:           Info
    def servers:        List[Server]
    def paths:          Map[String, Path]
  }

  object OpenApiDocument {
    def apply(openApiVersion: String, info: Info): DocWithInfo =
      DocWithInfo(openApiVersion, info, Nil, Map.empty)
  }

  private[model] case class DocWithInfo(openApiVersion: String,
                                        info:           Info,
                                        servers:        List[Server],
                                        paths:          Map[String, Path]
  ) extends OpenApiDocument {
    def addServer(server: Server): DocWithServers = DocWithServers(openApiVersion, info, servers = List(server), paths)
  }

  private case class DocWithServers(openApiVersion: String,
                                    info:           Info,
                                    servers:        List[Server],
                                    paths:          Map[String, Path]
  ) {
    def addPath(path: Path): CompleteDoc =
      CompleteDoc(openApiVersion, info, servers, paths + (path.template -> path))
  }

  private case class CompleteDoc(openApiVersion: String, info: Info, servers: List[Server], paths: Map[String, Path])
      extends OpenApiDocument {
    def addPath(path: Path): OpenApiDocument = copy(paths = paths + (path.template -> path))
  }

  case class Info(title: String, description: Option[String], version: String)

  case class Server(url: String, description: String, variables: Map[String, Variable])

  case class Variable(default: String)

  private[model] case class Uri(parts: List[UriPart]) {
    def show: Show[Uri] = Show.show(_ => parts.map(_.show).mkString("/"))
    def /(nextPart: Parameter) = copy(parts :+ ParameterPart(nextPart))
    def /(nextPart: String)    = copy(parts :+ StringPart(nextPart))
  }

  object Uri {
    def /(firstPart: Parameter) = Uri(List(ParameterPart(firstPart)))
    def /(firstPart: String)    = Uri(List(StringPart(firstPart)))
  }

  trait UriPart {
    def show: Show[UriPart]
  }

  case class StringPart(value: String) extends UriPart {
    def show: Show[UriPart] = Show.show(_ => value)
  }

  case class ParameterPart(parameter: Parameter) extends UriPart {
    def show: Show[UriPart] = Show.show(_ => s"{${parameter.name}}")
  }

  trait Path {
    def summary:     Option[String]
    def description: Option[String]
    def operations:  List[Operation]
    def parameters:  List[Parameter]
    def template:    String
  }

  object Path {
    def apply(summary: Option[String], description: Option[String]): PathWithDescriptors =
      PathWithDescriptors(summary: Option[String], description: Option[String])
  }

  private[model] case class PathWithDescriptors(summary: Option[String], description: Option[String]) {
    def addUri(uri: Uri): PathWithUri = {

      val parameters = uri.parts.flatMap {
        case ParameterPart(parameter) => Some(parameter)
        case _                        => None
      }

      val template = "/" + uri.parts
        .map {
          case ParameterPart(parameter) => s"{${parameter.name}}"
          case StringPart(value)        => value
        }
        .mkString("/")

      PathWithUri(summary, description, parameters, template)
    }
  }

  private case class PathWithUri(summary:     Option[String],
                                 description: Option[String],
                                 parameters:  List[Parameter],
                                 template:    String
  ) {
    def addGet(operation: Operation.Get): PathImpl =
      PathImpl(summary, description, List(operation), parameters, template)
  }

  private case class PathImpl(summary:     Option[String],
                              description: Option[String],
                              operations:  List[Operation],
                              parameters:  List[Parameter],
                              template:    String
  ) extends Path {
    def addGet(operation: Operation.Get): PathImpl =
      PathImpl(summary, description, operations :+ operation, parameters, template)
  }

  sealed trait Operation {
    def summary:     Option[String]
    def parameters:  List[Parameter]
    def requestBody: Option[RequestBody]
    def responses:   Map[String, Response]
    def security:    List[SecurityRequirement]
  }

  object Operation {
    case class Get(summary:     Option[String],
                   parameters:  List[Parameter],
                   requestBody: Option[RequestBody],
                   responses:   Map[String, Response],
                   security:    List[SecurityRequirement]
    ) extends Operation
  }

  case class Parameter(name: String, in: In, description: Option[String], required: Boolean, schema: Schema) {}

  sealed trait In extends Product with Serializable {
    def value: String
  }

  object In {

    final case object Query extends In {
      def value: String = "query"
    }

    final case object Header extends In {
      def value: String = "header"
    }

    final case object Path extends In {
      def value: String = "path"
    }

    final case object Cookie extends In {
      def value: String = "cookie"
    }

  }

  final case class RequestBody(description: String, content: Map[String, MediaType])
  final case class MediaType()
  final case class Response(description: String,
                            headers:     Map[String, Header],
                            content:     Map[String, MediaType],
                            links:       Map[String, Link]
  )
  final case class SecurityRequirement()
  final case class Header()
  final case class Link()
  final case class Schema(`type`: String)

}
