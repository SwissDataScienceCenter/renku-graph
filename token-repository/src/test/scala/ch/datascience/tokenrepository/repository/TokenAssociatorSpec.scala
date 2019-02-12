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

import ch.datascience.db.DbSpec
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.ProjectId
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class TokenAssociatorSpec extends WordSpec with DbSpec with InMemoryProjectsTokens {

  "associate" should {

    allTokenTypes foreach { tokenType =>
      s"succeed if token does not exist - $tokenType type case" in new TestCase {

        val encryptedToken = nonEmptyStrings().generateOne

        associator.associate(projectId, encryptedToken, tokenType).unsafeRunSync shouldBe ()

        findToken(projectId) shouldBe Some(encryptedToken)
      }

      s"succeed if token exists - $tokenType type case" in new TestCase {

        val encryptedToken = nonEmptyStrings().generateOne

        associator.associate(projectId, encryptedToken, tokenType).unsafeRunSync shouldBe ()

        findToken(projectId) shouldBe Some(encryptedToken)

        val newEncryptedToken = nonEmptyStrings().generateOne
        associator.associate(projectId, newEncryptedToken, tokenType).unsafeRunSync shouldBe ()

        findToken(projectId) shouldBe Some(newEncryptedToken)
      }
    }
  }

  private lazy val allTokenTypes = Set(TokenType.OAuth, TokenType.Personal)

  private trait TestCase {

    val projectId = projectIds.generateOne

    val associator = new TokenAssociator(transactorProvider)

    def findToken(projectId: ProjectId): Option[String] = {
      val finder = new TokenInRepoFinder(transactorProvider)
      finder.findToken(projectId).value.unsafeRunSync().map(_._1)
    }
  }
}
