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

package ch.datascience.tokenrepository.repository.association

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.tokenrepository.repository.InMemoryProjectsTokensDbSpec
import ch.datascience.tokenrepository.repository.RepositoryGenerators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class AssociationPersisterSpec extends WordSpec with InMemoryProjectsTokensDbSpec {

  "persistAssociation" should {

    "succeed if token does not exist" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne

      associator.persistAssociation(projectId, encryptedToken).unsafeRunSync shouldBe ()

      findToken(projectId) shouldBe Some(encryptedToken.value)
    }

    "succeed if token exists" in new TestCase {

      val encryptedToken = encryptedAccessTokens.generateOne

      associator.persistAssociation(projectId, encryptedToken).unsafeRunSync shouldBe ()

      findToken(projectId) shouldBe Some(encryptedToken.value)

      val newEncryptedToken = encryptedAccessTokens.generateOne
      associator.persistAssociation(projectId, newEncryptedToken).unsafeRunSync shouldBe ()

      findToken(projectId) shouldBe Some(newEncryptedToken.value)
    }
  }

  private trait TestCase {

    val projectId = projectIds.generateOne

    val associator = new AssociationPersister(transactorProvider)
  }
}
