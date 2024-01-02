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

package io.renku.tokenrepository.repository.creation

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.localDates
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectSlugs}
import io.renku.graph.model.projects
import io.renku.tokenrepository.repository.RepositoryGenerators.encryptedAccessTokens
import io.renku.tokenrepository.repository.creation.TokenDates.ExpiryDate
import io.renku.tokenrepository.repository.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.tokenrepository.repository.{ProjectsTokensDB, TokenRepositoryPostgresSpec}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import java.time.{LocalDate, Period}

class TokenDueCheckerSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with TokenRepositoryPostgresSpec
    with should.Matchers
    with AsyncMockFactory {

  it should "return true if token expires in tokenDuePeriod" in testDBResource.use { implicit cfg =>
    val projectId = projectIds.generateOne

    insertToken(projectId, LocalDate.now() plus tokenDuePeriod) >>
      new TokenDueCheckerImpl[IO](tokenDuePeriod).checkTokenDue(projectId).asserting(_ shouldBe true)
  }

  it should "return true if token expires in less than tokenDuePeriod" in testDBResource.use { implicit cfg =>
    val projectId = projectIds.generateOne

    insertToken(projectId, localDates(max = LocalDate.now() plus tokenDuePeriod minusDays 1).generateOne) >>
      new TokenDueCheckerImpl[IO](tokenDuePeriod).checkTokenDue(projectId).asserting(_ shouldBe true)
  }

  it should "return false if token expires in more than tokenDuePeriod" in testDBResource.use { implicit cfg =>
    val projectId = projectIds.generateOne

    insertToken(projectId, localDates(min = LocalDate.now() plus tokenDuePeriod plusDays 1).generateOne) >>
      new TokenDueCheckerImpl[IO](tokenDuePeriod).checkTokenDue(projectId).asserting(_ shouldBe false)
  }

  it should "return false if there's not token for the projectId" in testDBResource.use { implicit cfg =>
    new TokenDueCheckerImpl[IO](tokenDuePeriod).checkTokenDue(projectIds.generateOne).asserting(_ shouldBe false)
  }

  private def insertToken(projectId: projects.GitLabId, expiryDate: ExpiryDate)(implicit
      cfg: DBConfig[ProjectsTokensDB]
  ) = insert(projectId, projectSlugs.generateOne, encryptedAccessTokens.generateOne, expiryDate)

  private lazy val tokenDuePeriod = Period.ofDays(5)
  private implicit lazy val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
}
