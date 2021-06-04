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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import PersonDetailsGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.booleans
import ch.datascience.graph.model.users.Username
import ch.datascience.rdfstore.entities._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails
import eu.timepit.refined.auto._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PersonsAndProjectMembersMatcherSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "merge" should {

    "supply each person with a GitLab Id when it is matched in the members list" in {
      forAll(
        personEntities(withoutGitLabId)
          .map(_.to[persondetails.Person])
          .toGeneratorOfNonEmptyList(maxElements = 20)
          .map(_.toList.toSet)
      ) { personsWithoutId =>
        val personsAndMembers = personsWithoutId map { person =>
          val member =
            if (booleans.generateOne) gitLabProjectMembers.generateOne.copy(name = person.name)
            else gitLabProjectMembers.generateOne.copy(username = Username(person.name.value))
          val personWithId = person.copy(maybeGitLabId = Some(member.id))
          personWithId -> member
        }

        matcher.merge(personsWithoutId,
                      personsAndMembers.map { case (_, projectMember) => projectMember }
        ) shouldBe personsAndMembers.map { case (personWithGitlabId, _) => personWithGitlabId }
      }
    }

    "leave person without GitLab Id when it there's no matching member" in {

      val personsWithoutId = personEntities(withoutGitLabId)
        .map(_.to[persondetails.Person])
        .generateNonEmptyList(minElements = 2)
        .toList
        .toSet
      val personsAndMembers = personsWithoutId.tail map { person =>
        val member =
          if (booleans.generateOne) gitLabProjectMembers.generateOne.copy(name = person.name)
          else gitLabProjectMembers.generateOne.copy(username = Username(person.name.value))
        val personWithId = person.copy(maybeGitLabId = Some(member.id))
        personWithId -> member
      }

      matcher.merge(personsWithoutId, personsAndMembers.map { case (_, projectMember) => projectMember }) shouldBe
        (personsAndMembers.map { case (personWithGitlabId, _) => personWithGitlabId } + personsWithoutId.head)
    }

    "do nothing if no persons given" in {
      matcher.merge(Set.empty, gitLabProjectMembers.generateNonEmptyList().toList.toSet) shouldBe Set.empty
    }

    "do nothing if no members given" in {
      val personsWithoutId = personEntities(withoutGitLabId)
        .map(_.to[persondetails.Person])
        .generateNonEmptyList(minElements = 2)
        .toList
        .toSet

      matcher.merge(personsWithoutId, Set.empty) shouldBe personsWithoutId
    }
  }

  private lazy val matcher = new PersonsAndProjectMembersMatcher()
}
