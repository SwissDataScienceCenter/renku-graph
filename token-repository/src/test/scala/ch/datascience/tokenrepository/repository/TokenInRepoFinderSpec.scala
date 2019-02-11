/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository.repository

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.ProjectId
import doobie.implicits._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class TokenInRepoFinderSpec extends WordSpec {

  "findToken" should {

    "return token associated with the projectId - Personal Access Token case" in new TestCase {

      val encryptedToken = nonEmptyStrings().generateOne
      val tokenType      = TokenType.Personal

      insert(projectId, encryptedToken, tokenType.value)

      finder.findToken(projectId).value.unsafeRunSync shouldBe Some(encryptedToken -> tokenType)
    }

    "return token associated with the projectId - OAuth Access Token case" in new TestCase {

      val encryptedToken = nonEmptyStrings().generateOne
      val tokenType      = TokenType.OAuth

      insert(projectId, encryptedToken, tokenType.value)

      finder.findToken(projectId).value.unsafeRunSync shouldBe Some(encryptedToken -> tokenType)
    }

    "return None if there's no token associated with the projectId" in new TestCase {
      finder.findToken(projectId).value.unsafeRunSync shouldBe None
    }

    "fail with an error if the token associated with the projectId is of unknown type" in new TestCase {
      val encryptedToken = nonEmptyStrings().generateOne

      insert(projectId, encryptedToken, "UNKNOWN_TYPE")

      val result = finder.findToken(projectId)

      intercept[RuntimeException] {
        result.value.unsafeRunSync
      }.getMessage shouldBe s"Unknown token type: UNKNOWN_TYPE for projectId: $projectId"
    }
  }

  private trait TestCase {

    ProjectsTokensInMemoryDb.assureProjectsTokensIsEmpty()

    val projectId = projectIds.generateOne

    private val transactorProvider = H2TransactorProvider
    import transactorProvider.transactor

    val finder = new TokenInRepoFinder(transactorProvider)

    def insert(projectId: ProjectId, accessToken: String, tokenType: String): Unit =
      sql"""insert into 
          projects_tokens (project_id, token, token_type) 
          values (${projectId.value}, $accessToken, $tokenType)
      """.update.run
        .map(assureInserted)
        .transact(transactor)
        .unsafeRunSync()

    private lazy val assureInserted: Int => Unit = {
      case 1 => ()
      case _ => fail("insertion problem")
    }
  }
}
