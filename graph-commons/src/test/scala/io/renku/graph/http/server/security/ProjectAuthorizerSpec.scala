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

package io.renku.graph.http.server.security

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabApiUrl
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects.Visibility._
import io.renku.graph.model.testentities._
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectAuthorizerSpec extends AnyWordSpec with IOSpec with InMemoryRdfStore with should.Matchers {

  "authorize" should {

    "succeed if the given project is public and there's no auth user" in new TestCase {
      val project = projectEntities(visibilityPublic).generateOne.copy(visibility = Public)

      loadToStore(project)

      authorizer.authorize(project.path, maybeAuthUser = None).value.unsafeRunSync() shouldBe ().asRight
    }

    "succeed if the given project is public and the user has rights for the project" in new TestCase {
      val authUser = authUsers.generateOne
      val project = projectEntities(visibilityPublic).generateOne.copy(
        visibility = Public,
        members = Set(personEntities.generateOne.copy(maybeGitLabId = authUser.id.some))
      )

      loadToStore(project)

      authorizer.authorize(project.path, authUser.some).value.unsafeRunSync() shouldBe ().asRight
    }

    "succeed if the given project is non-public and the user has rights for the project" in new TestCase {
      val authUser = authUsers.generateOne
      val project = projectEntities(visibilityNonPublic).generateOne.copy(
        visibility = Gen.oneOf(Private, Internal).generateOne,
        members = Set(personEntities.generateOne.copy(maybeGitLabId = authUser.id.some))
      )

      loadToStore(project)

      authorizer.authorize(project.path, authUser.some).value.unsafeRunSync() shouldBe ().asRight
    }

    "succeed if there's no project with the given id" in new TestCase {
      authorizer
        .authorize(
          projectPaths.generateOne,
          authUsers.generateOption
        )
        .value
        .unsafeRunSync() shouldBe ().asRight
    }

    "fail if the given project is non-public and the user does not have rights for it" in new TestCase {
      val project = projectEntities(visibilityNonPublic).generateOne.copy(
        visibility = Gen.oneOf(Private, Internal).generateOne,
        members = personEntities.generateFixedSizeSet()
      )

      loadToStore(project)

      authorizer
        .authorize(project.path, authUsers.generateSome)
        .value
        .unsafeRunSync() shouldBe AuthorizationFailure.asLeft
    }

    "fail if the given project is non-public and there's no authorized user" in new TestCase {
      val project = projectEntities(visibilityNonPublic).generateOne.copy(
        visibility = Gen.oneOf(Private, Internal).generateOne,
        members = personEntities.generateFixedSizeSet()
      )

      loadToStore(project)

      authorizer
        .authorize(project.path, maybeAuthUser = None)
        .value
        .unsafeRunSync() shouldBe AuthorizationFailure.asLeft
    }
  }

  private implicit lazy val gitLabApiUrl: GitLabApiUrl = gitLabUrls.generateOne.apiV4

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder[IO]())
    val authorizer           = new ProjectAuthorizerImpl(rdfStoreConfig, timeRecorder)
  }
}
