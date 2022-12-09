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

package io.renku.knowledgegraph.users.projects
package finder

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.{authUsers, userAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects
import io.renku.graph.model.testentities._
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TSProjectFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with IOSpec {

  "findProjectsInTS" should {

    "return info about all projects where the user from criteria is either a member or a user of" in new TestCase {

      val criteria = {
        val c = criterias.generateOne
        c.copy(maybeUser = authUsers.generateOne.copy(id = c.userId).some)
      }

      val matchingMember = personEntities(withGitLabId).generateOne.copy(maybeGitLabId = criteria.userId.some)
      val project1WithMatchingMember = anyProjectEntities
        .map(replaceMembers(Set(personEntities(withGitLabId).generateOne, matchingMember)))
        .generateOne
      val project2WithMatchingMember = anyProjectEntities
        .map(replaceMembers(personEntities(withGitLabId).generateSet() + matchingMember))
        .generateOne

      val projectWithoutMatchingMember = projectEntities(visibilityPublic).generateOne

      upload(to = projectsDataset, project1WithMatchingMember, project2WithMatchingMember, projectWithoutMatchingMember)

      finder.findProjectsInTS(criteria).unsafeRunSync() should contain theSameElementsAs
        List(project1WithMatchingMember, project2WithMatchingMember).map(_.to[model.Project.Activated])
    }

    "not see projects the authUser has no access to" in new TestCase {

      val authUser = personEntities(withGitLabId).generateOne
      val criteria = criterias.generateOne.copy(maybeUser =
        AuthUser(authUser.maybeGitLabId.getOrElse(fail("AuthUser without GL id")), userAccessTokens.generateOne).some
      )

      val matchingMember = personEntities(withGitLabId).generateOne.copy(maybeGitLabId = criteria.userId.some)
      val privateProjectWithMatchingMemberAndAuthUser = projectEntities(visibilityPrivate)
        .map(replaceMembers(Set(authUser, matchingMember)))
        .generateOne
      val privateProjectWithMatchingMemberOnly = projectEntities(visibilityPrivate)
        .map(replaceMembers(personEntities(withGitLabId).generateSet() + matchingMember))
        .generateOne
      val nonPrivateProjectWithMatchingMemberOnly =
        projectEntities(Gen.oneOf(projects.Visibility.Public, projects.Visibility.Internal))
          .map(replaceMembers(personEntities(withGitLabId).generateSet() + matchingMember))
          .generateOne
      val projectWithMatchingAuthUserOnly = anyProjectEntities
        .map(replaceMembers(Set(authUser)))
        .generateOne

      upload(
        to = projectsDataset,
        privateProjectWithMatchingMemberAndAuthUser,
        privateProjectWithMatchingMemberOnly,
        nonPrivateProjectWithMatchingMemberOnly,
        projectWithMatchingAuthUserOnly
      )

      finder.findProjectsInTS(criteria).unsafeRunSync() should contain theSameElementsAs
        List(privateProjectWithMatchingMemberAndAuthUser, nonPrivateProjectWithMatchingMemberOnly)
          .map(_.to[model.Project.Activated])
    }

    "return no projects if there are no projects where the criteria user is a member of" in new TestCase {

      val criteria = {
        val c = criterias.generateOne
        c.copy(maybeUser = authUsers.generateOne.copy(id = c.userId).some)
      }

      upload(to = projectsDataset, projectEntities(visibilityPublic).generateOne)

      finder.findProjectsInTS(criteria).unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val finder = new TSProjectFinderImpl[IO](projectsDSConnectionInfo)
  }
}
