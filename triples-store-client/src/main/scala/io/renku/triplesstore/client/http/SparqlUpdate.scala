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

package io.renku.triplesstore.client.http

import org.http4s.EntityEncoder
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._

trait SparqlUpdate {
  def render: String
}

object SparqlUpdate {
  final case class Raw(render: String) extends SparqlUpdate

  def raw(sparql: String): SparqlUpdate = Raw(sparql)

  implicit def entityEncoder[F[_]]: EntityEncoder[F, SparqlUpdate] =
    EntityEncoder
      .stringEncoder[F]
      .contramap[SparqlUpdate](_.render)
      .withContentType(`Content-Type`(mediaType"application/sparql-update"))
}
