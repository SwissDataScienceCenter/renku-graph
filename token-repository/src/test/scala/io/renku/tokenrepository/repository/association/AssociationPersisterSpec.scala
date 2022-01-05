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
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.InMemoryProjectsTokensDbSpec
import io.renku.tokenrepository.repository.RepositoryGenerators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class AssociationPersisterSpec extends AnyWordSpec with IOSpec with InMemoryProjectsTokensDbSpec with should.Matchers {

  "persistAssociation" should {

    "insert the given token " +
      "if no token for the given project path" in new TestCase {

        val encryptedToken = encryptedAccessTokens.generateOne

        associator.persistAssociation(projectId, projectPath, encryptedToken).unsafeRunSync() shouldBe ()

        findToken(projectId)   shouldBe Some(encryptedToken.value)
        findToken(projectPath) shouldBe Some(encryptedToken.value)
      }

    "update the given token " +
      "if there's a token for the project path and id" in new TestCase {

        val encryptedToken = encryptedAccessTokens.generateOne

        associator.persistAssociation(projectId, projectPath, encryptedToken).unsafeRunSync() shouldBe ()

        findToken(projectId) shouldBe Some(encryptedToken.value)

        val newEncryptedToken = encryptedAccessTokens.generateOne
        associator.persistAssociation(projectId, projectPath, newEncryptedToken).unsafeRunSync() shouldBe ()

        findToken(projectId)   shouldBe Some(newEncryptedToken.value)
        findToken(projectPath) shouldBe Some(newEncryptedToken.value)
      }

    "update the given token and project id" +
      "if there's a token for the project path but with different project id" in new TestCase {

        val encryptedToken = encryptedAccessTokens.generateOne

        associator.persistAssociation(projectId, projectPath, encryptedToken).unsafeRunSync() shouldBe ()

        findToken(projectId) shouldBe Some(encryptedToken.value)

        val newProjectId      = projectIds.generateOne
        val newEncryptedToken = encryptedAccessTokens.generateOne
        associator.persistAssociation(newProjectId, projectPath, newEncryptedToken).unsafeRunSync() shouldBe ()

        findToken(projectId)    shouldBe None
        findToken(newProjectId) shouldBe Some(newEncryptedToken.value)
        findToken(projectPath)  shouldBe Some(newEncryptedToken.value)
      }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    private val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val associator               = new AssociationPersisterImpl[IO](sessionResource, queriesExecTimes)
  }
}
