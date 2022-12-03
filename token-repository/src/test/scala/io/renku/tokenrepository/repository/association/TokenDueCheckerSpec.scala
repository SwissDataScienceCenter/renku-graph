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

package io.renku.tokenrepository.repository.association

import cats.effect.IO
import io.renku.db.SqlStatement
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.localDates
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.projects
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.InMemoryProjectsTokensDbSpec
import io.renku.tokenrepository.repository.RepositoryGenerators.encryptedAccessTokens
import io.renku.tokenrepository.repository.association.TokenDates.ExpiryDate
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{LocalDate, Period}

class TokenDueCheckerSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryProjectsTokensDbSpec
    with should.Matchers
    with MockFactory {

  "checkTokenDue" should {

    "return true if token expires in tokenDuePeriod" in new TestCase {

      val projectId = projectIds.generateOne
      insertToken(projectId, LocalDate.now() plus tokenDuePeriod)

      dueChecker.checkTokenDue(projectId).unsafeRunSync() shouldBe true
    }

    "return true if token expires in less than tokenDuePeriod" in new TestCase {

      val projectId = projectIds.generateOne
      insertToken(projectId, localDates(max = LocalDate.now() plus tokenDuePeriod minusDays 1).generateOne)

      dueChecker.checkTokenDue(projectId).unsafeRunSync() shouldBe true
    }

    "return false if token expires in more than tokenDuePeriod" in new TestCase {

      val projectId = projectIds.generateOne
      insertToken(projectId, localDates(min = LocalDate.now() plus tokenDuePeriod plusDays 1).generateOne)

      dueChecker.checkTokenDue(projectId).unsafeRunSync() shouldBe false
    }

    "return false if there's not token for the projectId" in new TestCase {
      dueChecker.checkTokenDue(projectIds.generateOne).unsafeRunSync() shouldBe false
    }
  }

  private trait TestCase {

    private val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val tokenDuePeriod           = Period.ofDays(5)
    val dueChecker               = new TokenDueCheckerImpl[IO](tokenDuePeriod, queriesExecTimes)
  }

  private def insertToken(projectId: projects.Id, expiryDate: ExpiryDate): Unit =
    insert(projectId, projectPaths.generateOne, encryptedAccessTokens.generateOne, expiryDate)
}
