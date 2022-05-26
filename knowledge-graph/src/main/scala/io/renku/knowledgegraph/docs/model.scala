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

object model {
  case class OpenApiDocument(openapi: String, info: Info, servers: List[Server], paths: Map[String, Path])

  case class Info(title: String, description: Option[String], version: String)

  case class Server(url: String, description: String, variables: Map[String, Variable])

  case class Variable(default: String)

  case class Path(summary:     Option[String],
                  description: Option[String],
                  get:         Option[Operation],
                  post:        Option[Operation],
                  put:         Option[Operation],
                  delete:      Option[Operation],
                  parameters:  List[Parameter]
  )

  case class Operation(summary:     Option[String],
                       parameters:  List[Parameter],
                       requestBody: Option[RequestBody],
                       responses:   Map[String, Response],
                       security:    List[SecurityRequirement]
  )

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
