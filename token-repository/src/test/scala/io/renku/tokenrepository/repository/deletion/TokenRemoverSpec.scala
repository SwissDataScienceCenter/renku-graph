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

package io.renku.tokenrepository.repository.deletion

import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.tokenrepository.repository.InMemoryProjectsTokensDbSpec
import io.renku.tokenrepository.repository.RepositoryGenerators.encryptedAccessTokens
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TokenRemoverSpec extends AnyWordSpec with InMemoryProjectsTokensDbSpec with should.Matchers {

  "delete" should {

    "succeed if token does not exist" in new TestCase {
      remover.delete(projectId).unsafeRunSync() shouldBe ((): Unit)
    }

    "succeed if token exists" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne
      insert(projectId, projectPath, encryptedToken)
      findToken(projectId) shouldBe Some(encryptedToken.value)

      remover.delete(projectId).unsafeRunSync() shouldBe ((): Unit)

      findToken(projectId) shouldBe None
    }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    private val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val remover                  = new TokenRemover(sessionResource, queriesExecTimes)
  }
}
