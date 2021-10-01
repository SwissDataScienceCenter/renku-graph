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

package ch.datascience.graph.model.testentities

import cats.syntax.all._
import ch.datascience.graph.model._
import ch.datascience.graph.model.users.{Affiliation, Email, GitLabId, Name}

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
  import io.renku.jsonld.syntax._

  implicit def toEntitiesPerson(implicit renkuBaseUrl: RenkuBaseUrl): Person => entities.Person = person =>
    entities.Person(
      users.ResourceId(person.asEntityId),
      person.name,
      person.maybeEmail,
      person.maybeAffiliation,
      person.maybeGitLabId
    )

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Person] =
    JsonLDEncoder.instance(_.to[entities.Person].asJsonLD)

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Person] =
    EntityIdEncoder.instance {
      case Person(_, _, _, Some(gitLabId)) => EntityId of users.ResourceId(gitLabId).show
      case Person(_, Some(email), _, _)    => EntityId of s"mailto:$email"
      case Person(name, _, _, _)           => EntityId of (renkuBaseUrl / "users" / name)
    }
}
