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

package io.renku.tokenrepository.repository.fetching

import cats.MonadError
import cats.data.OptionT
import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{Id, Path}
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.IOAccessTokenCrypto
import io.renku.tokenrepository.repository.RepositoryGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TokenFinderSpec extends AnyWordSpec with MockFactory with should.Matchers {

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

      tokenFinder.findToken(projectId).value.unsafeRunSync() shouldBe Some(accessToken)
    }

    "return None if no token was found in the db" in new TestCase {

      val projectId = projectIds.generateOne

      (tokenInRepoFinder
        .findToken(_: Id))
        .expects(projectId)
        .returning(OptionT.none[IO, EncryptedAccessToken])

      tokenFinder.findToken(projectId).value.unsafeRunSync() shouldBe None
    }

    "fail if finding token in the db fails" in new TestCase {

      val projectId = projectIds.generateOne

      val exception = exceptions.generateOne
      (tokenInRepoFinder
        .findToken(_: Id))
        .expects(projectId)
        .returning(OptionT.liftF[IO, EncryptedAccessToken](context.raiseError(exception)))

      intercept[Exception] {
        tokenFinder.findToken(projectId).value.unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
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
      intercept[Exception] {
        tokenFinder.findToken(projectId).value.unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
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

      tokenFinder.findToken(projectPath).value.unsafeRunSync() shouldBe Some(accessToken)
    }

    "return None if no token was found in the db" in new TestCase {

      val projectPath = projectPaths.generateOne

      (tokenInRepoFinder
        .findToken(_: Path))
        .expects(projectPath)
        .returning(OptionT.none[IO, EncryptedAccessToken])

      tokenFinder.findToken(projectPath).value.unsafeRunSync() shouldBe None
    }

    "fail if finding token in the db fails" in new TestCase {

      val projectPath = projectPaths.generateOne

      val exception = exceptions.generateOne
      (tokenInRepoFinder
        .findToken(_: Path))
        .expects(projectPath)
        .returning(OptionT.liftF[IO, EncryptedAccessToken](context.raiseError(exception)))
      intercept[Exception] {
        tokenFinder.findToken(projectPath).value.unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
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

      intercept[Exception] {
        tokenFinder.findToken(projectPath).value.unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val accessTokenCrypto = mock[IOAccessTokenCrypto]
    val tokenInRepoFinder = mock[IOPersistedTokensFinder]
    val tokenFinder       = new TokenFinder[IO](tokenInRepoFinder, accessTokenCrypto)
  }
}
