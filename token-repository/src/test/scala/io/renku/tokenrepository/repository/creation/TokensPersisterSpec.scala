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

package io.renku.tokenrepository.repository
package creation

import AccessTokenCrypto.EncryptedAccessToken
import RepositoryGenerators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import creation.Generators.tokenStoringInfos
import creation.TokenDates._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.tokenrepository.repository.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import skunk._
import skunk.implicits._

class TokensPersisterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with TokenRepositoryPostgresSpec
    with should.Matchers
    with AsyncMockFactory {

  "persistToken" should {

    "insert the association " +
      "if there's no token for the given project id" in testDBResource.use { implicit cfg =>
        val tokenStoringInfo = tokenStoringInfos.generateOne

        TokensPersister[IO].persistToken(tokenStoringInfo).assertNoException >>
          findTokenInfo(tokenStoringInfo.project.id).asserting(_ shouldBe tokenStoringInfo.some)
      }

    "update the given token " +
      "if there's a token for the project slug and id" in testDBResource.use { implicit cfg =>
        val tokenStoringInfo = tokenStoringInfos.generateOne

        for {
          _ <- TokensPersister[IO].persistToken(tokenStoringInfo).assertNoException
          _ <- findTokenInfo(tokenStoringInfo.project.id).asserting(_ shouldBe tokenStoringInfo.some)

          newToken = encryptedAccessTokens.generateOne
          _ <- TokensPersister[IO].persistToken(tokenStoringInfo.copy(encryptedToken = newToken)).assertNoException
          res <- findTokenInfo(tokenStoringInfo.project.id)
                   .asserting(_ shouldBe tokenStoringInfo.copy(encryptedToken = newToken).some)
        } yield res
      }

    "update the given token and project id" +
      "if there's a token for the project slug but with different project id" in testDBResource.use { implicit cfg =>
        val tokenStoringInfo = tokenStoringInfos.generateOne

        for {
          _ <- TokensPersister[IO].persistToken(tokenStoringInfo).assertNoException
          _ <- findTokenInfo(tokenStoringInfo.project.id).asserting(_ shouldBe tokenStoringInfo.some)

          newStoringInfo = tokenStoringInfo.copy(project = tokenStoringInfo.project.copy(id = projectIds.generateOne),
                                                 encryptedToken = encryptedAccessTokens.generateOne
                           )
          _ <- TokensPersister[IO].persistToken(newStoringInfo).assertNoException

          _   <- findTokenInfo(tokenStoringInfo.project.id).asserting(_ shouldBe None)
          res <- findTokenInfo(newStoringInfo.project.id).asserting(_ shouldBe newStoringInfo.some)
        } yield res
      }

    "update the given token and project slug" +
      "if there's a token for the project id but with different project slug" in testDBResource.use { implicit cfg =>
        val tokenStoringInfo = tokenStoringInfos.generateOne

        for {
          _ <- TokensPersister[IO].persistToken(tokenStoringInfo).assertNoException
          _ <- findTokenInfo(tokenStoringInfo.project.id).asserting(_ shouldBe tokenStoringInfo.some)

          newStoringInfo = tokenStoringInfo.copy(project =
                                                   tokenStoringInfo.project.copy(slug = projectSlugs.generateOne),
                                                 encryptedToken = encryptedAccessTokens.generateOne
                           )
          _ <- TokensPersister[IO].persistToken(newStoringInfo).assertNoException

          res <- findTokenInfo(tokenStoringInfo.project.id).asserting(_ shouldBe newStoringInfo.some)
        } yield res
      }
  }

  "updateSlug" should {

    "replace the Slug for the given project Id" in testDBResource.use { implicit cfg =>
      val tokenStoringInfo = tokenStoringInfos.generateOne

      for {
        _ <- TokensPersister[IO].persistToken(tokenStoringInfo).assertNoException
        _ <- findTokenInfo(tokenStoringInfo.project.id).asserting(_ shouldBe tokenStoringInfo.some)

        newSlug = projectSlugs.generateOne
        _ <- TokensPersister[IO].updateSlug(Project(tokenStoringInfo.project.id, newSlug)).assertNoException
        res <- findTokenInfo(tokenStoringInfo.project.id).asserting(
                 _ shouldBe tokenStoringInfo
                   .copy(project = tokenStoringInfo.project.copy(slug = newSlug))
                   .some
               )
      } yield res
    }

    "do nothing if project Slug not changed" in testDBResource.use { implicit cfg =>
      val tokenStoringInfo = tokenStoringInfos.generateOne

      TokensPersister[IO].persistToken(tokenStoringInfo).assertNoException >>
        TokensPersister[IO].updateSlug(tokenStoringInfo.project).assertNoException >>
        findTokenInfo(tokenStoringInfo.project.id).asserting(_ shouldBe tokenStoringInfo.some)
    }

    "do nothing if no project with the given Id" in testDBResource.use { implicit cfg =>
      val tokenStoringInfo = tokenStoringInfos.generateOne

      TokensPersister[IO].updateSlug(tokenStoringInfo.project).assertNoException >>
        findTokenInfo(tokenStoringInfo.project.id).asserting(_ shouldBe None)
    }
  }

  private def findTokenInfo(
      projectId: projects.GitLabId
  )(implicit cfg: DBConfig[ProjectsTokensDB]): IO[Option[TokenStoringInfo]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[projects.GitLabId, TokenStoringInfo] = sql"""
      SELECT project_id, project_slug, token, created_at, expiry_date
      FROM projects_tokens
      WHERE project_id = $projectIdEncoder"""
        .query(
          projectIdDecoder ~ projectSlugDecoder ~ encryptedAccessTokenDecoder ~ createdAtDecoder ~ expiryDateDecoder
        )
        .map {
          case (id: projects.GitLabId) ~ (slug: projects.Slug) ~ (token: EncryptedAccessToken) ~ (createdAt: CreatedAt) ~ (expiryDate: ExpiryDate) =>
            TokenStoringInfo(Project(id, slug), token, TokenDates(createdAt, expiryDate))
        }
      session.prepare(query).flatMap(_.option(projectId))
    }

  private implicit lazy val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
}
