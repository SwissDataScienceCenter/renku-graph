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

package io.renku.tokenrepository.repository.fetching

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.InMemoryProjectsTokensDbSpec
import io.renku.tokenrepository.repository.RepositoryGenerators._
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class PersistedTokensFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryProjectsTokensDbSpec
    with should.Matchers
    with MockFactory {

  "findStoredToken" should {

    "return token associated with the projectId" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne

      insert(projectId, projectSlug, encryptedToken)

      finder.findStoredToken(projectId).value.unsafeRunSync() shouldBe Some(encryptedToken)
    }

    "return token associated with the projectSlug" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne

      insert(projectId, projectSlug, encryptedToken)

      finder.findStoredToken(projectSlug).value.unsafeRunSync() shouldBe Some(encryptedToken)
    }

    "return None if there's no token associated with the projectId" in new TestCase {
      finder.findStoredToken(projectId).value.unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val projectSlug = projectSlugs.generateOne

    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val finder = new PersistedTokensFinderImpl[IO]
  }
}
