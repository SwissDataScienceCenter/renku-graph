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

package ch.datascience.graph.http.server.security

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.GraphModelGenerators.{authUsers, projectPaths}
import ch.datascience.graph.model.projects.Visibility._
import ch.datascience.http.server.security.EndpointSecurityException.AuthorizationFailure
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.EntitiesGenerators._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.rdfstore.entities.Project

class ProjectAuthorizerSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "authorize" should {

    "succeed if the given project is public and there's no auth user" in new TestCase {
      val project = projectEntities[Project.ForksCount.Zero](visibilityPublic).generateOne.copy(
        visibility = Public
      )

      loadToStore(project.asJsonLD)

      authorizer.authorize(project.path, maybeAuthUser = None).value.unsafeRunSync() shouldBe Right(())
    }

    "succeed if the given project is public and the user has rights for the project" in new TestCase {
      val authUser = authUsers.generateOne
      val project = projectEntities[Project.ForksCount.Zero](visibilityPublic).generateOne.copy(
        visibility = Public,
        members = Set(personEntities.generateOne.copy(maybeGitLabId = authUser.id.some))
      )

      loadToStore(project.asJsonLD)

      authorizer.authorize(project.path, authUser.some).value.unsafeRunSync() shouldBe Right(())
    }

    "succeed if the given project is non-public and the user has rights for the project" in new TestCase {
      val authUser = authUsers.generateOne
      val project = projectEntities[Project.ForksCount.Zero](visibilityNonPublic).generateOne.copy(
        visibility = Gen.oneOf(Private, Internal).generateOne,
        members = Set(personEntities.generateOne.copy(maybeGitLabId = authUser.id.some))
      )

      loadToStore(project.asJsonLD)

      authorizer.authorize(project.path, authUser.some).value.unsafeRunSync() shouldBe Right(())
    }

    "succeed if there's no project with the given id" in new TestCase {
      authorizer
        .authorize(
          projectPaths.generateOne,
          authUsers.generateOption
        )
        .value
        .unsafeRunSync() shouldBe Right(())
    }

    "fail if the given project is non-public and the user does not have rights for it" in new TestCase {
      val project = projectEntities[Project.ForksCount.Zero](visibilityNonPublic).generateOne.copy(
        visibility = Gen.oneOf(Private, Internal).generateOne,
        members = personEntities.generateFixedSizeSet()
      )

      loadToStore(project.asJsonLD)

      authorizer.authorize(project.path, authUsers.generateSome).value.unsafeRunSync() shouldBe Left(
        AuthorizationFailure
      )
    }

    "fail if the given project is non-public and there's no authorized user" in new TestCase {
      val project = projectEntities[Project.ForksCount.Zero](visibilityNonPublic).generateOne.copy(
        visibility = Gen.oneOf(Private, Internal).generateOne,
        members = personEntities.generateFixedSizeSet()
      )

      loadToStore(project.asJsonLD)

      authorizer.authorize(project.path, maybeAuthUser = None).value.unsafeRunSync() shouldBe Left(
        AuthorizationFailure
      )
    }
  }

  private implicit lazy val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne
  private implicit lazy val gitLabApiUrl: GitLabApiUrl = gitLabUrls.generateOne.apiV4

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val authorizer           = new ProjectAuthorizerImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }
}
