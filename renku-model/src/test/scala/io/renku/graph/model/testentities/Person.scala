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
import io.renku.graph.model.users.{Affiliation, Email, GitLabId, Name}

final case class Person(
    name:             Name,
    maybeEmail:       Option[Email] = None,
    maybeAffiliation: Option[Affiliation] = None,
    maybeGitLabId:    Option[GitLabId] = None
)

object Person {

  def apply(name: Name, email: Email): Person = Person(name, Some(email))

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def toEntitiesPerson(implicit renkuBaseUrl: RenkuBaseUrl): Person => entities.Person = {
    case Person(name, maybeEmail, maybeAffiliation, Some(gitLabId)) =>
      entities.Person.WithGitLabId(users.ResourceId(gitLabId), gitLabId, name, maybeEmail, maybeAffiliation)
    case Person(name, Some(email), maybeAffiliation, None) =>
      entities.Person.WithEmail(users.ResourceId(email), name, email, maybeAffiliation)
    case Person(name, None, maybeAffiliation, None) =>
      entities.Person.WithNameOnly(users.ResourceId(name), name, maybeAffiliation)
  }

  implicit def toMaybeEntitiesPersonWithGitLabId(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): Person => Option[entities.Person.WithGitLabId] = {
    case Person(name, maybeEmail, maybeAffiliation, Some(gitLabId)) =>
      entities.Person.WithGitLabId(users.ResourceId(gitLabId), gitLabId, name, maybeEmail, maybeAffiliation).some
    case _ => None
  }

  implicit lazy val toMaybeEntitiesPersonWithEmail: Person => Option[entities.Person.WithEmail] = {
    case Person(name, Some(email), maybeAffiliation, None) =>
      entities.Person.WithEmail(users.ResourceId(email), name, email, maybeAffiliation).some
    case _ => None
  }

  implicit def toMaybeEntitiesPersonWithName(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): Person => Option[entities.Person.WithNameOnly] = {
    case Person(name, None, maybeAffiliation, None) =>
      entities.Person.WithNameOnly(users.ResourceId(name), name, maybeAffiliation).some
    case _ => None
  }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Person] =
    JsonLDEncoder.instance(_.to[entities.Person].asJsonLD)

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Person] =
    EntityIdEncoder.instance {
      case Person(_, _, _, Some(gitLabId)) => EntityId of users.ResourceId(gitLabId).show
      case Person(_, Some(email), _, _)    => EntityId of users.ResourceId(email).show
      case Person(name, _, _, _)           => EntityId of users.ResourceId(name).show
    }
}
