/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.users.projects
package finder

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.RenkuTinyTypeGenerators._
import io.renku.graph.model.persons
import io.renku.http.client.AccessToken
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class GLCreatorsNamesAdderSpec extends AnyWordSpec with should.Matchers with MockFactory with TryValues {

  "find names of all the distinct creators found among the non activated projects " +
    "and add them to the projects" in new TestCase {

      val commonCreatorId        = personGitLabIds.generateOne
      val nonActivated1          = notActivatedProjects.generateOne.copy(maybeCreatorId = commonCreatorId.some)
      val nonActivated2          = notActivatedProjects.generateOne.copy(maybeCreatorId = commonCreatorId.some)
      val nonActivated3          = notActivatedProjects.generateOne.copy(maybeCreatorId = None, maybeCreator = None)
      val nonActivated4CreatorId = personGitLabIds.generateOne
      val nonActivated4          = notActivatedProjects.generateOne.copy(maybeCreatorId = nonActivated4CreatorId.some)
      val activated              = activatedProjects.generateOne

      val commonCreatorName = personNames.generateSome
      givenCreatorFinding(commonCreatorId, returning = commonCreatorName.pure[Try])
      val nonActivated4CreatorName = personNames.generateSome
      givenCreatorFinding(nonActivated4CreatorId, returning = nonActivated4CreatorName.pure[Try])

      adder
        .addCreatorsNames(criteria)(
          nonActivated1 :: nonActivated2 :: nonActivated3 :: nonActivated4 :: activated :: Nil
        )
        .success
        .value shouldBe nonActivated1.copy(maybeCreator = commonCreatorName) ::
        nonActivated2.copy(maybeCreator = commonCreatorName) ::
        nonActivated3 ::
        nonActivated4.copy(maybeCreator = nonActivated4CreatorName) ::
        activated :: Nil
    }

  "fail if finding names fails" in new TestCase {

    val creatorId = personGitLabIds.generateOne
    val project   = notActivatedProjects.generateOne.copy(maybeCreatorId = creatorId.some)

    val exception = exceptions.generateOne
    givenCreatorFinding(creatorId, returning = exception.raiseError[Try, Option[persons.Name]])

    adder
      .addCreatorsNames(criteria)(project :: Nil)
      .failure
      .exception shouldBe exception
  }

  private trait TestCase {

    val criteria = criterias.generateOne

    private val glCreatorFinder = mock[GLCreatorFinder[Try]]
    val adder                   = new GLCreatorsNamesAdderImpl[Try](glCreatorFinder)

    def givenCreatorFinding(creatorId: persons.GitLabId, returning: Try[Option[persons.Name]]) =
      (glCreatorFinder
        .findCreatorName(_: persons.GitLabId)(_: Option[AccessToken]))
        .expects(creatorId, criteria.maybeUser.map(_.accessToken))
        .returning(returning)
  }
}
