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

import java.util.UUID

import ch.datascience.graph.model.users.{Affiliation, Email, Name}
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.Json
import io.circe.literal._

object Person {

  def apply(
      id:               Id,
      userName:         Name,
      maybeAffiliation: Option[Affiliation] = None
  ): Json = apply(id, userName, id.maybeEmail, maybeAffiliation)

  // format: off
  def apply(
      id:               Id,
      userName:         Name,
      maybeEmail:       Option[Email],
      maybeAffiliation: Option[Affiliation]
  ): Json = json"""
  {
    "@id": $id,
    "@type": [
      "http://schema.org/Person",
      "http://www.w3.org/ns/prov#Person"
    ],
    "http://schema.org/name": ${userName.toValue},
    "http://www.w3.org/2000/01/rdf-schema#label": ${userName.toValue}
  }""".deepMerge(maybeEmail toValue "http://schema.org/email")
      .deepMerge(maybeAffiliation toValue "http://schema.org/affiliation")
  // format: on

  final case class Id(maybeEmail: Option[Email]) extends EntityId {
    override val value: String = maybeEmail match {
      case Some(email) => s"mailto:$email"
      case None        => s"_:${UUID.randomUUID()}"
    }
  }
}
