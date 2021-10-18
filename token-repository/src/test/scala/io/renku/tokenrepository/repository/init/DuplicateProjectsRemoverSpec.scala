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

package io.renku.tokenrepository.repository.init

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.tokenrepository.repository.InMemoryProjectsTokensDbSpec
import io.renku.tokenrepository.repository.RepositoryGenerators.encryptedAccessTokens
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DuplicateProjectsRemoverSpec extends AnyWordSpec with InMemoryProjectsTokensDbSpec with should.Matchers {

  "run" should {

    "de-duplicate rows with the same project_path but different ids" in new TestCase {

      val projectPath     = projectPaths.generateOne
      val projectId1      = projects.Id(1)
      val encryptedToken1 = encryptedAccessTokens.generateOne
      insert(projectId1, projectPath, encryptedToken1)
      val projectId2      = projects.Id(2)
      val encryptedToken2 = encryptedAccessTokens.generateOne
      insert(projectId2, projectPath, encryptedToken2)
      val projectPath3    = projectPaths.generateOne
      val encryptedToken3 = encryptedAccessTokens.generateOne
      insert(projects.Id(3), projectPath3, encryptedToken3)

      findToken(projectId1) shouldBe Some(encryptedToken1.value)
      findToken(projectId2) shouldBe Some(encryptedToken2.value)

      deduplicator.run().unsafeRunSync() shouldBe ((): Unit)

      findToken(projectId1)   shouldBe None
      findToken(projectId2)   shouldBe Some(encryptedToken2.value)
      findToken(projectPath)  shouldBe Some(encryptedToken2.value)
      findToken(projectPath3) shouldBe Some(encryptedToken3.value)

      logger.loggedOnly(Info("Projects de-duplicated"))
    }
  }

  private trait TestCase {
    val logger       = TestLogger[IO]()
    val deduplicator = new DuplicateProjectsRemoverImpl[IO](sessionResource, logger)
  }
}
