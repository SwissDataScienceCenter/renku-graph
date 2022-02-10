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

package io.renku.graph.http.server.security

import cats.data.EitherT
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.http.server.security.Authorizer.{AuthContext, SecurityRecord}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.testentities._
import io.renku.graph.model.users.GitLabId
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import org.scalacheck.Arbitrary
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class AuthorizerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "authorize" should {

    "succeed if found SecurityRecord is for a public project and there's no auth user" in new TestCase {
      val projectPath = projectPaths.generateOne
      securityRecordsFinder
        .expects(key)
        .returning(List((Visibility.Public, projectPath, Set.empty[GitLabId])).pure[Try])

      authorizer.authorize(key, maybeAuthUser = None) shouldBe EitherT.rightT[Try, EndpointSecurityException](
        AuthContext[Key](None, key, Set(projectPath))
      )
    }

    "succeed if found SecurityRecord is for a public project and the user has rights to the project" in new TestCase {
      val projectPath = projectPaths.generateOne
      val authUser    = authUsers.generateOne

      securityRecordsFinder
        .expects(key)
        .returning(List((Visibility.Public, projectPath, userGitLabIds.generateSet() + authUser.id)).pure[Try])

      authorizer.authorize(key, authUser.some) shouldBe EitherT.rightT[Try, EndpointSecurityException](
        AuthContext[Key](Some(authUser), key, Set(projectPath))
      )
    }

    "succeed if found SecurityRecord is for a public project and the user has no explicit rights to the project" in new TestCase {
      val projectPath = projectPaths.generateOne
      val authUser    = authUsers.generateOne

      securityRecordsFinder
        .expects(key)
        .returning(List((Visibility.Public, projectPath, userGitLabIds.generateSet())).pure[Try])

      authorizer.authorize(key, authUser.some) shouldBe EitherT.rightT[Try, EndpointSecurityException](
        AuthContext[Key](Some(authUser), key, Set(projectPath))
      )
    }

    "succeed if found SecurityRecord is for a non-public project but the user has rights to the project" in new TestCase {
      val projectPath = projectPaths.generateOne
      val authUser    = authUsers.generateOne

      securityRecordsFinder
        .expects(key)
        .returning(
          List((visibilityNonPublic.generateOne, projectPath, userGitLabIds.generateSet() + authUser.id)).pure[Try]
        )

      authorizer.authorize(key, authUser.some) shouldBe EitherT.rightT[Try, EndpointSecurityException](
        AuthContext[Key](Some(authUser), key, Set(projectPath))
      )
    }

    "succeed with no allowed projects if there's no project with the given id" in new TestCase {
      val authUser = authUsers.generateOne

      securityRecordsFinder.expects(key).returning(Nil.pure[Try])

      authorizer.authorize(key, Some(authUser)) shouldBe EitherT.rightT[Try, EndpointSecurityException](
        AuthContext[Key](Some(authUser), key, Set.empty)
      )
    }

    "fail if the given project is non-public and the user does not have rights for it" in new TestCase {

      securityRecordsFinder
        .expects(key)
        .returning(
          List((visibilityNonPublic.generateOne, projectPaths.generateOne, userGitLabIds.generateSet())).pure[Try]
        )

      authorizer.authorize(key, authUsers.generateSome) shouldBe EitherT.leftT[Try, Unit](AuthorizationFailure)
    }

    "fail if the given project is non-public and there's no authorized user" in new TestCase {
      securityRecordsFinder
        .expects(key)
        .returning(
          List((visibilityNonPublic.generateOne, projectPaths.generateOne, userGitLabIds.generateSet())).pure[Try]
        )

      authorizer.authorize(key, maybeAuthUser = None) shouldBe EitherT.leftT[Try, Unit](AuthorizationFailure)
    }
  }

  private case class Key(value: Int)

  private trait TestCase {
    val key                   = Arbitrary.arbInt.arbitrary.map(Key).generateOne
    val securityRecordsFinder = mockFunction[Key, Try[List[SecurityRecord]]]
    val authorizer            = new AuthorizerImpl[Try, Key](securityRecordsFinder)
  }
}
