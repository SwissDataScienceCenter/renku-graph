/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.rdfstore.triples
package entities

import ch.datascience.graph.model.users.{Email, Name}
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.Json
import io.circe.literal._

object Person {

  def apply(id: Id, maybeEmail: Option[Email]): Json = json"""
  {
    "@id": $id,
    "@type": [
      "schema:Person",
      "prov:Person"
    ],
    "schema:name": ${id.userName},
    "rdfs:label": ${id.userName}
  }""" deepMerge (maybeEmail to "schema:email")

  final case class Id(userName: Name) extends EntityId {
    override val value: String = s"file:///_${userName.value.replace(" ", "-")}"
  }
}
