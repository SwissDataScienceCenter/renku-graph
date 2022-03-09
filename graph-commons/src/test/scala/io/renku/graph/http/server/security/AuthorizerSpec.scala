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

import cats.data.EitherT.{leftT, rightT}
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.http.server.security.Authorizer.{AuthContext, SecurityRecord}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.users.GitLabId
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import org.scalacheck.Arbitrary
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class AuthorizerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "authorize - public projects" should {

    "succeed if found SecurityRecord is for a public project and there's no auth user" in new TestCase {
      val projectPath = projectPaths.generateOne
      securityRecordsFinder
        .expects(key)
        .returning(List((Visibility.Public, projectPath, Set.empty[GitLabId])).pure[Try])

      authorizer.authorize(key, maybeAuthUser = None) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](None, key, Set(projectPath))
      )
    }

    "succeed if found SecurityRecord is for a public project and the user has rights to the project" in new TestCase {
      val projectPath = projectPaths.generateOne
      val authUser    = authUsers.generateOne

      securityRecordsFinder
        .expects(key)
        .returning(List((Visibility.Public, projectPath, userGitLabIds.generateSet() + authUser.id)).pure[Try])

      authorizer.authorize(key, authUser.some) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](Some(authUser), key, Set(projectPath))
      )
    }

    "succeed if found SecurityRecord is for a public project and the user has no explicit rights to the project" in new TestCase {
      val projectPath = projectPaths.generateOne
      val authUser    = authUsers.generateOne

      securityRecordsFinder
        .expects(key)
        .returning(List((Visibility.Public, projectPath, userGitLabIds.generateSet())).pure[Try])

      authorizer.authorize(key, authUser.some) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](Some(authUser), key, Set(projectPath))
      )
    }
  }

  "authorize - non-public projects" should {

    Visibility.Internal :: Visibility.Private :: Nil foreach { visibility =>
      s"fail if found SecurityRecord is for a $visibility project and there's no auth user" in new TestCase {
        val projectPath = projectPaths.generateOne
        securityRecordsFinder
          .expects(key)
          .returning(List((visibility, projectPath, Set.empty[GitLabId])).pure[Try])

        authorizer.authorize(key, maybeAuthUser = None) shouldBe leftT[Try, Unit](AuthorizationFailure)
      }
    }

    Visibility.Internal :: Visibility.Private :: Nil foreach { visibility =>
      s"succeed if found SecurityRecord is for a $visibility project but the user has rights to the project" in new TestCase {
        val projectPath = projectPaths.generateOne
        val authUser    = authUsers.generateOne

        securityRecordsFinder
          .expects(key)
          .returning(List((visibility, projectPath, userGitLabIds.generateSet() + authUser.id)).pure[Try])

        authorizer.authorize(key, authUser.some) shouldBe rightT[Try, EndpointSecurityException](
          AuthContext[Key](Some(authUser), key, Set(projectPath))
        )
      }
    }

    "succeed if found SecurityRecord is for an internal project even if the user has no explicit rights for the project" in new TestCase {
      val projectPath = projectPaths.generateOne
      val authUser    = authUsers.generateOne

      securityRecordsFinder
        .expects(key)
        .returning(List((Visibility.Internal, projectPath, userGitLabIds.generateSet())).pure[Try])

      authorizer.authorize(key, authUser.some) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](Some(authUser), key, Set(projectPath))
      )
    }

    "fail if found SecurityRecord is for a private project and the user has no explicit rights for the project" in new TestCase {
      val projectPath = projectPaths.generateOne
      val authUser    = authUsers.generateOne

      securityRecordsFinder
        .expects(key)
        .returning(List((Visibility.Private, projectPath, userGitLabIds.generateSet())).pure[Try])

      authorizer.authorize(key, authUser.some) shouldBe leftT[Try, Unit](AuthorizationFailure)
    }

    "fail if there's no project with the given id" in new TestCase {
      val authUser = authUsers.generateOne

      securityRecordsFinder.expects(key).returning(Nil.pure[Try])

      authorizer.authorize(key, Some(authUser)) shouldBe leftT[Try, Unit](AuthorizationFailure)
    }
  }

  "authorize - auth records for projects with mixed visibility" should {

    "succeed with public projects only if there's no auth user" in new TestCase {
      val publicProject = projectPaths.generateOne
      securityRecordsFinder
        .expects(key)
        .returning(
          List(
            (Visibility.Public, publicProject, userGitLabIds.generateSet()),
            (Visibility.Internal, projectPaths.generateOne, userGitLabIds.generateSet()),
            (Visibility.Private, projectPaths.generateOne, userGitLabIds.generateSet())
          ).pure[Try]
        )

      authorizer.authorize(key, maybeAuthUser = None) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](maybeAuthUser = None, key, Set(publicProject))
      )
    }

    "succeed with public and internal projects only if there's an auth user without explicit access for the private projects" in new TestCase {
      val authUser = authUsers.generateOne

      val publicProject   = projectPaths.generateOne
      val internalProject = projectPaths.generateOne
      securityRecordsFinder
        .expects(key)
        .returning(
          List(
            (Visibility.Public, publicProject, userGitLabIds.generateSet()),
            (Visibility.Internal, internalProject, userGitLabIds.generateSet()),
            (Visibility.Private, projectPaths.generateOne, userGitLabIds.generateSet())
          ).pure[Try]
        )

      authorizer.authorize(key, authUser.some) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](authUser.some, key, Set(publicProject, internalProject))
      )
    }

    "succeed with public, internal and private projects if there's an auth user with explicit access for the private projects" in new TestCase {
      val authUser = authUsers.generateOne

      val publicProject   = projectPaths.generateOne
      val internalProject = projectPaths.generateOne
      val privateProject  = projectPaths.generateOne
      securityRecordsFinder
        .expects(key)
        .returning(
          List(
            (Visibility.Public, publicProject, userGitLabIds.generateSet()),
            (Visibility.Internal, internalProject, userGitLabIds.generateSet()),
            (Visibility.Private, privateProject, userGitLabIds.generateSet() + authUser.id)
          ).pure[Try]
        )

      authorizer.authorize(key, authUser.some) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](authUser.some, key, Set(publicProject, internalProject, privateProject))
      )
    }
  }

  private case class Key(value: Int)

  private trait TestCase {
    val key                   = Arbitrary.arbInt.arbitrary.map(Key).generateOne
    val securityRecordsFinder = mockFunction[Key, Try[List[SecurityRecord]]]
    val authorizer            = new AuthorizerImpl[Try, Key](securityRecordsFinder)
  }
}
