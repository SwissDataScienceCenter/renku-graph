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

package io.renku.tokenrepository.repository
package creation

import AccessTokenCrypto.EncryptedAccessToken
import RepositoryGenerators._
import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import creation.Generators.tokenStoringInfos
import creation.TokenDates._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._

class TokensPersisterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryProjectsTokensDbSpec
    with should.Matchers
    with MockFactory {

  "persistToken" should {

    "insert the association " +
      "if there's no token for the given project id" in new TestCase {

        persister.persistToken(tokenStoringInfo).unsafeRunSync() shouldBe ()

        findTokenInfo(tokenStoringInfo.project.id) shouldBe tokenStoringInfo.some
      }

    "update the given token " +
      "if there's a token for the project path and id" in new TestCase {

        persister.persistToken(tokenStoringInfo).unsafeRunSync() shouldBe ()

        findTokenInfo(tokenStoringInfo.project.id) shouldBe tokenStoringInfo.some

        val newToken = encryptedAccessTokens.generateOne
        persister.persistToken(tokenStoringInfo.copy(encryptedToken = newToken)).unsafeRunSync() shouldBe ()

        findTokenInfo(tokenStoringInfo.project.id) shouldBe tokenStoringInfo.copy(encryptedToken = newToken).some
      }

    "update the given token and project id" +
      "if there's a token for the project path but with different project id" in new TestCase {

        persister.persistToken(tokenStoringInfo).unsafeRunSync() shouldBe ()

        findTokenInfo(tokenStoringInfo.project.id) shouldBe tokenStoringInfo.some

        val newStoringInfo =
          tokenStoringInfo.copy(project = tokenStoringInfo.project.copy(id = projectIds.generateOne),
                                encryptedToken = encryptedAccessTokens.generateOne
          )
        persister.persistToken(newStoringInfo).unsafeRunSync() shouldBe ()

        findTokenInfo(tokenStoringInfo.project.id) shouldBe None
        findTokenInfo(newStoringInfo.project.id)   shouldBe newStoringInfo.some
      }

    "update the given token and project path" +
      "if there's a token for the project id but with different project path" in new TestCase {

        persister.persistToken(tokenStoringInfo).unsafeRunSync() shouldBe ()

        findTokenInfo(tokenStoringInfo.project.id) shouldBe tokenStoringInfo.some

        val newStoringInfo =
          tokenStoringInfo.copy(project = tokenStoringInfo.project.copy(path = projectPaths.generateOne),
                                encryptedToken = encryptedAccessTokens.generateOne
          )
        persister.persistToken(newStoringInfo).unsafeRunSync() shouldBe ()

        findTokenInfo(tokenStoringInfo.project.id) shouldBe newStoringInfo.some
      }
  }

  "updatePath" should {

    "replace the Path for the given project Id" in new TestCase {

      persister.persistToken(tokenStoringInfo).unsafeRunSync() shouldBe ()

      findTokenInfo(tokenStoringInfo.project.id) shouldBe tokenStoringInfo.some

      val newPath = projectPaths.generateOne
      persister.updatePath(Project(tokenStoringInfo.project.id, newPath)).unsafeRunSync() shouldBe ()

      findTokenInfo(tokenStoringInfo.project.id) shouldBe tokenStoringInfo
        .copy(project = tokenStoringInfo.project.copy(path = newPath))
        .some
    }

    "do nothing if project Path not changed" in new TestCase {

      persister.persistToken(tokenStoringInfo).unsafeRunSync() shouldBe ()

      persister.updatePath(tokenStoringInfo.project).unsafeRunSync() shouldBe ()

      findTokenInfo(tokenStoringInfo.project.id) shouldBe tokenStoringInfo.some
    }

    "do nothing if no project with the given Id" in new TestCase {

      persister.updatePath(tokenStoringInfo.project).unsafeRunSync() shouldBe ()

      findTokenInfo(tokenStoringInfo.project.id) shouldBe None
    }
  }

  private trait TestCase {
    val tokenStoringInfo = tokenStoringInfos.generateOne

    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val persister = new TokensPersisterImpl[IO]
  }

  private def findTokenInfo(projectId: projects.Id): Option[TokenStoringInfo] = sessionResource
    .useK {
      val query: Query[projects.Id, TokenStoringInfo] = sql"""
      SELECT project_id, project_path, token, created_at, expiry_date
      FROM projects_tokens
      WHERE project_id = $projectIdEncoder"""
        .query(
          projectIdDecoder ~ projectPathDecoder ~ encryptedAccessTokenDecoder ~ createdAtDecoder ~ expiryDateDecoder
        )
        .map {
          case (id: projects.Id) ~ (path: projects.Path) ~ (token: EncryptedAccessToken) ~ (createdAt: CreatedAt) ~ (expiryDate: ExpiryDate) =>
            TokenStoringInfo(Project(id, path), token, TokenDates(createdAt, expiryDate))
        }
      Kleisli(_.prepare(query).use(_.option(projectId)))
    }
    .unsafeRunSync()
}
