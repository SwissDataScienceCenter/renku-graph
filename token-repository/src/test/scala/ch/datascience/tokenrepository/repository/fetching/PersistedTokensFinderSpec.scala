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

package ch.datascience.tokenrepository.repository.fetching

import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.metrics.TestLabeledHistogram
import ch.datascience.tokenrepository.repository.InMemoryProjectsTokensDbSpec
import ch.datascience.tokenrepository.repository.RepositoryGenerators._
import eu.timepit.refined.auto._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class PersistedTokensFinderSpec extends AnyWordSpec with InMemoryProjectsTokensDbSpec with should.Matchers {

  "findToken" should {

    "return token associated with the projectId" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne

      insert(projectId, projectPath, encryptedToken)

      finder.findToken(projectId).value.unsafeRunSync() shouldBe Some(encryptedToken)
    }

    "return token associated with the projectPath" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne

      insert(projectId, projectPath, encryptedToken)

      finder.findToken(projectPath).value.unsafeRunSync() shouldBe Some(encryptedToken)
    }

    "return None if there's no token associated with the projectId" in new TestCase {
      finder.findToken(projectId).value.unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    private val queriesExecTimes = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val finder                   = new PersistedTokensFinder(sessionResource, queriesExecTimes)
  }
}
