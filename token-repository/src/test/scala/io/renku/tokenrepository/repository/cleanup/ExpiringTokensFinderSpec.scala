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

package io.renku.tokenrepository.repository
package cleanup

import AccessTokenCrypto.EncryptedAccessToken
import RepositoryGenerators.encryptedAccessTokens
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import creation.TokenDates.ExpiryDate
import deletion.PersistedTokenRemover
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import java.time.LocalDate.now
import java.time.Period

class ExpiringTokensFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with TokenRepositoryPostgresSpec
    with should.Matchers
    with AsyncMockFactory {

  it should "return a stream of ExpiringToken objects for all the projects with tokens expiring in less than the given period" in testDBResource
    .use { implicit cfg =>
      for {
        proj1 <- insertDecryptableToken(projectIds.generateOne, expiryDate = ExpiryDate(now().plusDays(1)))
        proj2 <- insertDecryptableToken(projectIds.generateOne, expiryDate = ExpiryDate(now().plus(beforeExpiration)))
        _ <- insertDecryptableToken(projectIds.generateOne,
                                    expiryDate = ExpiryDate(now().plus(beforeExpiration.plusDays(1)))
             )
        res <- finder.findExpiringTokens
                 .evalTap(pt => PersistedTokenRemover[IO].delete(pt.project.id))
                 .map(_.project.id)
                 .compile
                 .toList
                 .asserting(_ shouldBe List(proj1, proj2))
      } yield res
    }

  it should "return a stream of ExpiringToken objects even if the token fails decryption" in testDBResource
    .use { implicit cfg =>
      for {
        proj1 <- insertNonDecryptableToken(projectIds.generateOne, expiryDate = ExpiryDate(now().plusDays(1)))
        proj2 <- insertDecryptableToken(projectIds.generateOne, expiryDate = ExpiryDate(now().plus(beforeExpiration)))
        _ <- insertDecryptableToken(projectIds.generateOne,
                                    expiryDate = ExpiryDate(now().plus(beforeExpiration.plusDays(1)))
             )
        res <- finder.findExpiringTokens
                 .evalTap(pt => PersistedTokenRemover[IO].delete(pt.project.id))
                 .map(_.project.id)
                 .compile
                 .toList
                 .asserting(_ shouldBe List(proj1, proj2))
      } yield res
    }

  private lazy val beforeExpiration = Period.ofDays(10)
  private implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
  private lazy val tokenCrypto = mock[AccessTokenCrypto[IO]]
  private def finder(implicit cfg: DBConfig[ProjectsTokensDB]) =
    new ExpiringTokensFinderImpl[IO](tokenCrypto, beforeExpiration, chunkSize = 1)

  private def givenSuccessfulDecryption(encToken: EncryptedAccessToken) =
    (tokenCrypto.decrypt _)
      .expects(encToken)
      .returning(accessTokens.generateOne.pure[IO])
      .anyNumberOfTimes()

  private def givenDecryption(encToken: EncryptedAccessToken, returning: IO[AccessToken]) =
    (tokenCrypto.decrypt _)
      .expects(encToken)
      .returning(returning)

  private def insertDecryptableToken(projectId: projects.GitLabId, expiryDate: ExpiryDate)(implicit
      cfg: DBConfig[ProjectsTokensDB]
  ): IO[projects.GitLabId] = {
    val encToken = encryptedAccessTokens.generateOne
    givenSuccessfulDecryption(encToken)
    insert(projectId, projectSlugs.generateOne, encToken, expiryDate).as(projectId)
  }

  private def insertNonDecryptableToken(projectId: projects.GitLabId, expiryDate: ExpiryDate)(implicit
      cfg: DBConfig[ProjectsTokensDB]
  ): IO[projects.GitLabId] = {
    val encToken = encryptedAccessTokens.generateOne
    givenDecryption(encToken, returning = exceptions.generateOne.raiseError[IO, Nothing])
    insert(projectId, projectSlugs.generateOne, encToken, expiryDate).as(projectId)
  }
}
