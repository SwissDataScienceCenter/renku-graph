/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users.{Affiliation, Email, GitLabId, Name}
import org.scalacheck.Gen

import java.util.UUID

final case class Person(
    name:             Name,
    maybeEmail:       Option[Email] = None,
    maybeAffiliation: Option[Affiliation] = None,
    maybeGitLabId:    Option[GitLabId] = None
)

object Person {

  def apply(
      name:  Name,
      email: Email
  ): Person = Person(name, Some(email))

  import io.renku.jsonld._
  import JsonLDEncoder._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Person] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entityId(entity),
        EntityTypes.of(prov / "Person", schema / "Person"),
        schema / "email"       -> entity.maybeEmail.asJsonLD,
        schema / "name"        -> entity.name.asJsonLD,
        rdfs / "label"         -> entity.name.asJsonLD,
        schema / "affiliation" -> entity.maybeAffiliation.asJsonLD,
        schema / "sameAs"      -> entity.maybeGitLabId.asJsonLD(encodeOption(gitLabIdEncoder))
      )
    }

  private def gitLabIdEncoder(implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[GitLabId] = JsonLDEncoder.instance {
    gitLabId =>
      JsonLD.entity(
        EntityId of (gitLabApiUrl / "users" / gitLabId).toString,
        EntityTypes.of(schema / "URL"),
        schema / "identifier"     -> gitLabId.value.asJsonLD,
        schema / "additionalType" -> "GitLab".asJsonLD
      )
  }

  private def entityId(person: Person)(implicit renkuBaseUrl: RenkuBaseUrl): EntityId = person.maybeEmail match {
    case Some(email) => EntityId of s"mailto:$email"
    case None        => EntityId of (renkuBaseUrl / "persons" / UUID.nameUUIDFromBytes(person.name.value.getBytes()).toString)
  }
}
