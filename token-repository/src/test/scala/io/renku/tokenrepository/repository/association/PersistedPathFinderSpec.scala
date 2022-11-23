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

import io.renku.db.SqlStatement
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.InMemoryProjectsTokensDbSpec
import io.renku.tokenrepository.repository.RepositoryGenerators.encryptedAccessTokens
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class PersistedPathFinderSpec extends AnyWordSpec with IOSpec with InMemoryProjectsTokensDbSpec with should.Matchers {

  "findPersistedProjectPath" should {

    "return Path for the given project Id" in new TestCase {

      val projectPath = projectPaths.generateOne

      insert(projectId, projectPath, encryptedAccessTokens.generateOne)

      (finder findPersistedProjectPath projectId).unsafeRunSync() shouldBe projectPath
    }

    "fail if there's no Path for the given Id" in new TestCase {
      intercept[Exception]{
        (finder findPersistedProjectPath projectId).unsafeRunSync()
      }
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne

    private val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val finder                   = new PersistedPathFinderImpl(queriesExecTimes)
  }
}
