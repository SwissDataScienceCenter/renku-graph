/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.projects.Visibility
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import org.scalacheck.Arbitrary
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import Authorizer._

import scala.util.Try

class AuthorizerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "authorize - public projects" should {

    "succeed if found SecurityRecord is for a public project and there's no auth user" in new TestCase {

      val projectSlug = projectSlugs.generateOne
      (securityRecordsFinder.apply _)
        .expects(key, None)
        .returning(List(SecurityRecord(Visibility.Public, projectSlug, Set.empty[GitLabId])).pure[Try])

      authorizer.authorize(key, maybeAuthUser = None) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](None, key, Set(projectSlug))
      )
    }

    "succeed if found SecurityRecord is for a public project and the user has rights to the project" in new TestCase {

      val projectSlug = projectSlugs.generateOne
      val authUser    = authUsers.generateOne

      (securityRecordsFinder.apply _)
        .expects(key, authUser.some)
        .returning(
          List(SecurityRecord(Visibility.Public, projectSlug, personGitLabIds.generateSet() + authUser.id)).pure[Try]
        )

      authorizer.authorize(key, authUser.some) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](authUser.some, key, Set(projectSlug))
      )
    }

    "succeed if found SecurityRecord is for a public project and the user has no explicit rights to the project" in new TestCase {

      val projectSlug = projectSlugs.generateOne
      val authUser    = authUsers.generateSome

      (securityRecordsFinder.apply _)
        .expects(key, authUser)
        .returning(List(SecurityRecord(Visibility.Public, projectSlug, personGitLabIds.generateSet())).pure[Try])

      authorizer.authorize(key, authUser) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](authUser, key, Set(projectSlug))
      )
    }
  }

  "authorize - non-public projects" should {

    Visibility.Internal :: Visibility.Private :: Nil foreach { visibility =>
      s"fail if found SecurityRecord is for a $visibility project and there's no auth user" in new TestCase {

        val projectSlug = projectSlugs.generateOne
        (securityRecordsFinder.apply _)
          .expects(key, None)
          .returning(List(SecurityRecord(visibility, projectSlug, Set.empty[GitLabId])).pure[Try])

        authorizer.authorize(key, maybeAuthUser = None) shouldBe leftT[Try, Unit](AuthorizationFailure)
      }
    }

    Visibility.Internal :: Visibility.Private :: Nil foreach { visibility =>
      s"succeed if found SecurityRecord is for a $visibility project but the user has rights to the project" in new TestCase {

        val projectSlug = projectSlugs.generateOne
        val authUser    = authUsers.generateOne

        (securityRecordsFinder.apply _)
          .expects(key, authUser.some)
          .returning(
            List(SecurityRecord(visibility, projectSlug, personGitLabIds.generateSet() + authUser.id)).pure[Try]
          )

        authorizer.authorize(key, authUser.some) shouldBe rightT[Try, EndpointSecurityException](
          AuthContext[Key](authUser.some, key, Set(projectSlug))
        )
      }
    }

    "succeed if found SecurityRecord is for an internal project even if the user has no explicit rights for the project" in new TestCase {

      val projectSlug = projectSlugs.generateOne
      val authUser    = authUsers.generateSome

      (securityRecordsFinder.apply _)
        .expects(key, authUser)
        .returning(List(SecurityRecord(Visibility.Internal, projectSlug, personGitLabIds.generateSet())).pure[Try])

      authorizer.authorize(key, authUser) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](authUser, key, Set(projectSlug))
      )
    }

    "fail if found SecurityRecord is for a private project and the user has no explicit rights for the project" in new TestCase {

      val projectSlug = projectSlugs.generateOne
      val authUser    = authUsers.generateSome

      (securityRecordsFinder.apply _)
        .expects(key, authUser)
        .returning(List(SecurityRecord(Visibility.Private, projectSlug, personGitLabIds.generateSet())).pure[Try])

      authorizer.authorize(key, authUser) shouldBe leftT[Try, Unit](AuthorizationFailure)
    }

    "fail if there's no project with the given id" in new TestCase {

      val authUser = authUsers.generateSome

      (securityRecordsFinder.apply _)
        .expects(key, authUser)
        .returning(Nil.pure[Try])

      authorizer.authorize(key, authUser) shouldBe leftT[Try, Unit](AuthorizationFailure)
    }
  }

  "authorize - auth records for projects with mixed visibility" should {

    "succeed with public projects only if there's no auth user" in new TestCase {
      val publicProject = projectSlugs.generateOne
      (securityRecordsFinder.apply _)
        .expects(key, None)
        .returning(
          List(
            SecurityRecord(Visibility.Public, publicProject, personGitLabIds.generateSet()),
            SecurityRecord(Visibility.Internal, projectSlugs.generateOne, personGitLabIds.generateSet()),
            SecurityRecord(Visibility.Private, projectSlugs.generateOne, personGitLabIds.generateSet())
          ).pure[Try]
        )

      authorizer.authorize(key, maybeAuthUser = None) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](maybeAuthUser = None, key, Set(publicProject))
      )
    }

    "succeed with public and internal projects only if there's an auth user without explicit access for the private projects" in new TestCase {

      val authUser = authUsers.generateOne

      val publicProject   = projectSlugs.generateOne
      val internalProject = projectSlugs.generateOne
      (securityRecordsFinder.apply _)
        .expects(key, authUser.some)
        .returning(
          List(
            SecurityRecord(Visibility.Public, publicProject, personGitLabIds.generateSet()),
            SecurityRecord(Visibility.Internal, internalProject, personGitLabIds.generateSet()),
            SecurityRecord(Visibility.Private, projectSlugs.generateOne, personGitLabIds.generateSet())
          ).pure[Try]
        )

      authorizer.authorize(key, authUser.some) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](authUser.some, key, Set(publicProject, internalProject))
      )
    }

    "succeed with public, internal and private projects if there's an auth user with explicit access for the private projects" in new TestCase {

      val authUser = authUsers.generateOne

      val publicProject   = projectSlugs.generateOne
      val internalProject = projectSlugs.generateOne
      val privateProject  = projectSlugs.generateOne
      (securityRecordsFinder.apply _)
        .expects(key, authUser.some)
        .returning(
          List(
            SecurityRecord(Visibility.Public, publicProject, personGitLabIds.generateSet()),
            SecurityRecord(Visibility.Internal, internalProject, personGitLabIds.generateSet()),
            SecurityRecord(Visibility.Private, privateProject, personGitLabIds.generateSet() + authUser.id)
          ).pure[Try]
        )

      authorizer.authorize(key, authUser.some) shouldBe rightT[Try, EndpointSecurityException](
        AuthContext[Key](authUser.some, key, Set(publicProject, internalProject, privateProject))
      )
    }
  }

  private case class Key(value: Int)

  private trait TestCase {
    val securityRecordsFinder = mock[SecurityRecordFinder[Try, Key]]
    val key                   = Arbitrary.arbInt.arbitrary.map(Key).generateOne
    val authorizer            = new AuthorizerImpl[Try, Key](securityRecordsFinder)
  }
}
