/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository.repository.fetching

import cats.MonadError
import cats.data.OptionT
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.http.client.AccessToken
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.RepositoryGenerators._
import ch.datascience.tokenrepository.repository.TryAccessTokenCrypto
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class TokenFinderSpec extends WordSpec with MockFactory {

  "findToken(ProjectId)" should {

    "return Access Token if the token found for the projectId in the db was successfully decrypted" in new TestCase {

      val projectId = projectIds.generateOne

      val encryptedToken = encryptedAccessTokens.generateOne
      (tokenInRepoFinder
        .findToken(_: Id))
        .expects(projectId)
        .returning(OptionT.some(encryptedToken))

      val accessToken = accessTokens.generateOne
      (accessTokenCrypto
        .decrypt(_: EncryptedAccessToken))
        .expects(encryptedToken)
        .returning(context.pure(accessToken))

      tokenFinder.findToken(projectId) shouldBe OptionT.some[Try](accessToken)
    }

    "return None if no token was found in the db" in new TestCase {

      val projectId = projectIds.generateOne

      (tokenInRepoFinder
        .findToken(_: Id))
        .expects(projectId)
        .returning(OptionT.none[Try, EncryptedAccessToken])

      tokenFinder.findToken(projectId) shouldBe OptionT.none[Try, AccessToken]
    }

    "fail if finding token in the db fails" in new TestCase {

      val projectId = projectIds.generateOne

      val exception = exceptions.generateOne
      (tokenInRepoFinder
        .findToken(_: Id))
        .expects(projectId)
        .returning(OptionT.liftF[Try, EncryptedAccessToken](context.raiseError(exception)))

      tokenFinder.findToken(projectId).value shouldBe context.raiseError(exception)
    }

    "fail if decrypting found token fails" in new TestCase {

      val projectId = projectIds.generateOne

      val encryptedToken = encryptedAccessTokens.generateOne
      (tokenInRepoFinder
        .findToken(_: Id))
        .expects(projectId)
        .returning(OptionT.some(encryptedToken))

      val exception = exceptions.generateOne
      (accessTokenCrypto
        .decrypt(_: EncryptedAccessToken))
        .expects(encryptedToken)
        .returning(context.raiseError(exception))

      tokenFinder.findToken(projectId).value shouldBe context.raiseError(exception)
    }
  }

  "findToken(ProjectPath)" should {

    "return Access Token if the token found for the projectPath in the db was successfully decrypted" in new TestCase {

      val projectPath = projectPaths.generateOne

      val encryptedToken = encryptedAccessTokens.generateOne
      (tokenInRepoFinder
        .findToken(_: Path))
        .expects(projectPath)
        .returning(OptionT.some(encryptedToken))

      val accessToken = accessTokens.generateOne
      (accessTokenCrypto
        .decrypt(_: EncryptedAccessToken))
        .expects(encryptedToken)
        .returning(context.pure(accessToken))

      tokenFinder.findToken(projectPath) shouldBe OptionT.some[Try](accessToken)
    }

    "return None if no token was found in the db" in new TestCase {

      val projectPath = projectPaths.generateOne

      (tokenInRepoFinder
        .findToken(_: Path))
        .expects(projectPath)
        .returning(OptionT.none[Try, EncryptedAccessToken])

      tokenFinder.findToken(projectPath) shouldBe OptionT.none[Try, AccessToken]
    }

    "fail if finding token in the db fails" in new TestCase {

      val projectPath = projectPaths.generateOne

      val exception = exceptions.generateOne
      (tokenInRepoFinder
        .findToken(_: Path))
        .expects(projectPath)
        .returning(OptionT.liftF[Try, EncryptedAccessToken](context.raiseError(exception)))

      tokenFinder.findToken(projectPath).value shouldBe context.raiseError(exception)
    }

    "fail if decrypting found token fails" in new TestCase {

      val projectPath = projectPaths.generateOne

      val encryptedToken = encryptedAccessTokens.generateOne
      (tokenInRepoFinder
        .findToken(_: Path))
        .expects(projectPath)
        .returning(OptionT.some(encryptedToken))

      val exception = exceptions.generateOne
      (accessTokenCrypto
        .decrypt(_: EncryptedAccessToken))
        .expects(encryptedToken)
        .returning(context.raiseError(exception))

      tokenFinder.findToken(projectPath).value shouldBe context.raiseError(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val accessTokenCrypto = mock[TryAccessTokenCrypto]
    val tokenInRepoFinder = mock[TryPersistedTokensFinder]
    val tokenFinder       = new TokenFinder[Try](tokenInRepoFinder, accessTokenCrypto)
  }
}
