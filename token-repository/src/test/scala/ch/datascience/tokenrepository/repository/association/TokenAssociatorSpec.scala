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

import cats.MonadError
import cats.implicits._
import ch.datascience.http.client.AccessToken
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.graph.events.ProjectId
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.RepositoryGenerators._
import ch.datascience.tokenrepository.repository.TryAccessTokenCrypto
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class TokenAssociatorSpec extends WordSpec with MockFactory {

  "associate" should {

    "succeed if token encryption and storing in the db is successful" in new TestCase {
      val encryptedAccessToken = encryptedAccessTokens.generateOne
      (accessTokenCrypto
        .encrypt(_: AccessToken))
        .expects(accessToken)
        .returning(context.pure(encryptedAccessToken))

      (associationPersister
        .persistAssociation(_: ProjectId, _: EncryptedAccessToken))
        .expects(projectId, encryptedAccessToken)
        .returning(context.pure(()))

      tokenAssociator.associate(projectId, accessToken) shouldBe context.pure(())
    }

    "fail if token encryption fails" in new TestCase {
      val exception = exceptions.generateOne
      (accessTokenCrypto
        .encrypt(_: AccessToken))
        .expects(accessToken)
        .returning(context.raiseError(exception))

      tokenAssociator.associate(projectId, accessToken) shouldBe context.raiseError(exception)
    }

    "fail if storing in the db fails" in new TestCase {
      val encryptedAccessToken = encryptedAccessTokens.generateOne
      (accessTokenCrypto
        .encrypt(_: AccessToken))
        .expects(accessToken)
        .returning(context.pure(encryptedAccessToken))

      val exception = exceptions.generateOne
      (associationPersister
        .persistAssociation(_: ProjectId, _: EncryptedAccessToken))
        .expects(projectId, encryptedAccessToken)
        .returning(context.raiseError(exception))

      tokenAssociator.associate(projectId, accessToken) shouldBe context.raiseError(exception)
    }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val accessToken = accessTokens.generateOne

    val context = MonadError[Try, Throwable]

    val accessTokenCrypto    = mock[TryAccessTokenCrypto]
    val associationPersister = mock[TryAssociationPersister]
    val tokenAssociator      = new TokenAssociator[Try](accessTokenCrypto, associationPersister)
  }
}
