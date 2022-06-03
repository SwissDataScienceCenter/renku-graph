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

package io.renku.graph.model.testentities

import cats.syntax.all._
import io.renku.graph.model._
import io.renku.graph.model.persons.{Affiliation, Email, GitLabId, Name, OrcidId}

final case class Person(
    name:             Name,
    maybeEmail:       Option[Email] = None,
    maybeGitLabId:    Option[GitLabId] = None,
    maybeOrcidId:     Option[OrcidId] = None,
    maybeAffiliation: Option[Affiliation] = None
)

object Person {

  def apply(name: Name, email: Email): Person = Person(name, Some(email))

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def toEntitiesPerson(implicit renkuBaseUrl: RenkuBaseUrl): Person => entities.Person = {
    case Person(name, maybeEmail, Some(gitLabId), maybeOrcidId, maybeAffiliation) =>
      entities.Person.WithGitLabId(persons.ResourceId(gitLabId),
                                   gitLabId,
                                   name,
                                   maybeEmail,
                                   maybeOrcidId,
                                   maybeAffiliation
      )
    case Person(name, Some(email), None, maybeOrcidId, maybeAffiliation) =>
      entities.Person.WithEmail(maybeOrcidId.map(persons.ResourceId(_)) getOrElse persons.ResourceId(email),
                                name,
                                email,
                                maybeOrcidId,
                                maybeAffiliation
      )
    case Person(name, None, None, maybeOrcidId, maybeAffiliation) =>
      entities.Person.WithNameOnly(maybeOrcidId.map(persons.ResourceId(_)) getOrElse persons.ResourceId(name),
                                   name,
                                   maybeOrcidId,
                                   maybeAffiliation
      )
  }

  implicit def toMaybeEntitiesPersonWithGitLabId(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): Person => Option[entities.Person.WithGitLabId] = {
    case Person(name, maybeEmail, Some(gitLabId), maybeOrcidId, maybeAffiliation) =>
      entities.Person
        .WithGitLabId(persons.ResourceId(gitLabId), gitLabId, name, maybeEmail, maybeOrcidId, maybeAffiliation)
        .some
    case _ => None
  }

  implicit lazy val toMaybeEntitiesPersonWithEmail: Person => Option[entities.Person.WithEmail] = {
    case Person(name, Some(email), None, maybeOrcidId, maybeAffiliation) =>
      entities.Person.WithEmail(persons.ResourceId(email), name, email, maybeOrcidId, maybeAffiliation).some
    case _ => None
  }

  implicit def toMaybeEntitiesPersonWithName(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): Person => Option[entities.Person.WithNameOnly] = {
    case Person(name, None, None, maybeOrcidId, maybeAffiliation) =>
      entities.Person.WithNameOnly(persons.ResourceId(name), name, maybeOrcidId, maybeAffiliation).some
    case _ => None
  }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Person] =
    JsonLDEncoder.instance(_.to[entities.Person].asJsonLD)

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Person] =
    EntityIdEncoder.instance(person => EntityId.of(person.to[entities.Person].resourceId.value))
}
