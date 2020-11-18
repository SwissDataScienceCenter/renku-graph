/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.rdfstore.entities

import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users.{Affiliation, Email, Name}
import org.scalacheck.Gen

final case class Person(
    name:             Name,
    maybeEmail:       Option[Email] = None,
    maybeAffiliation: Option[Affiliation] = None
)

object Person {

  def apply(
      name:  Name,
      email: Email
  ): Person = Person(name, Some(email))

  import java.util.UUID.randomUUID

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[Person] = JsonLDEncoder.instance { entity =>
    JsonLD.entity(
      entityId(entity.maybeEmail),
      EntityTypes.of(prov / "Person", schema / "Person"),
      schema / "email"       -> entity.maybeEmail.asJsonLD,
      schema / "name"        -> entity.name.asJsonLD,
      rdfs / "label"         -> entity.name.asJsonLD,
      schema / "affiliation" -> entity.maybeAffiliation.asJsonLD
    )
  }

  private def entityId(maybeEmail: Option[Email])(implicit renkuBaseUrl: RenkuBaseUrl): EntityId = maybeEmail match {
    case Some(email) => EntityId of s"mailto:$email"
    case None        => EntityId of (renkuBaseUrl / "persons" / randomUUID().toString)
  }

  val persons: Gen[Person] = for {
    name             <- userNames
    maybeEmail       <- Gen.option(userEmails)
    maybeAffiliation <- Gen.option(userAffiliations)
  } yield Person(name, maybeEmail, maybeAffiliation)
}
