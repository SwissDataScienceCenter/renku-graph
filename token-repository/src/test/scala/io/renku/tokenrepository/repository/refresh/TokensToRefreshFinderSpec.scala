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
package refresh

import RepositoryGenerators._
import association.Generators._
import cats.effect.IO
import io.renku.db.SqlStatement
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.localDates
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.InMemoryProjectsTokensDbSpec
import io.renku.tokenrepository.repository.association.TokenDates.ExpiryDate
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate

class TokensToRefreshFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryProjectsTokensDbSpec
    with should.Matchers
    with MockFactory {

  "findTokenToRefresh" should {

    "return an event for tokens with expiration date <= today + 1 day" in new TestCase {

      insert(projectIds.generateOne,
             projectPaths.generateOne,
             encryptedAccessTokens.generateOne,
             localDates(min = LocalDate.now().plusDays(2)).generateAs(ExpiryDate)
      )

      val project1        = projectObjects.generateOne
      val encryptedToken1 = encryptedAccessTokens.generateOne
      insert(project1.id, project1.path, encryptedToken1, ExpiryDate(LocalDate.now()))

      val project2        = projectObjects.generateOne
      val encryptedToken2 = encryptedAccessTokens.generateOne
      insert(project2.id, project2.path, encryptedToken2, localDates(max = LocalDate.now()).generateAs(ExpiryDate))

      val Some(foundEvent1) = finder.findTokenToRefresh().unsafeRunSync()
      deleteToken(foundEvent1.project.id)

      val Some(foundEvent2) = finder.findTokenToRefresh().unsafeRunSync()
      deleteToken(foundEvent2.project.id)

      finder.findTokenToRefresh().unsafeRunSync() shouldBe None

      findToken(project1.id) shouldBe None
      findToken(project2.id) shouldBe None
    }
  }

  private trait TestCase {
    private val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val finder                   = new TokensToRefreshFinderImpl[IO](queriesExecTimes)
  }
}
