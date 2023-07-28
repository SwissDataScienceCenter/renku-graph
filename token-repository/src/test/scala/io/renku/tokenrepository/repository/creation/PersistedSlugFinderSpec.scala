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

package io.renku.tokenrepository.repository.creation

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.InMemoryProjectsTokensDbSpec
import io.renku.tokenrepository.repository.RepositoryGenerators.encryptedAccessTokens
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class PersistedSlugFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryProjectsTokensDbSpec
    with should.Matchers
    with MockFactory {

  "findPersistedProjectSlug" should {

    "return Slug for the given project Id" in new TestCase {

      val projectSlug = projectSlugs.generateOne

      insert(projectId, projectSlug, encryptedAccessTokens.generateOne)

      (finder findPersistedProjectSlug projectId).unsafeRunSync() shouldBe projectSlug
    }

    "fail if there's no Slug for the given Id" in new TestCase {
      intercept[Exception] {
        (finder findPersistedProjectSlug projectId).unsafeRunSync()
      }
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne

    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val finder = new PersistedSlugFinderImpl[IO]
  }
}
