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

package ch.datascience.triplesgenerator.events.categories

import cats.syntax.all._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.users.ResourceId
import ch.datascience.rdfstore.entities.Person
import io.renku.jsonld.syntax._

package object membersync {

  private[membersync] implicit class PersonsOps(persons: Set[Person])(implicit
      renkuBaseUrl:                                      RenkuBaseUrl,
      gitLabApiUrl:                                      GitLabApiUrl
  ) {

    lazy val toKGProjectMembers: Set[KGProjectMember] = persons.flatMap { member =>
      (member.asJsonLD.entityId.map(ResourceId.apply) -> member.maybeGitLabId)
        .mapN { case (resourceId, gitLabId) =>
          KGProjectMember(resourceId, gitLabId)
        }
    }
  }
}
