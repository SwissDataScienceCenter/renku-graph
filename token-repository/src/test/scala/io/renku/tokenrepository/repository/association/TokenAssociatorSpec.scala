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

package io.renku.tokenrepository.repository.association

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects.{Id, Path}
import io.renku.http.client.AccessToken
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.AccessTokenCrypto
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.RepositoryGenerators._
import io.renku.tokenrepository.repository.deletion.TokenRemover
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TokenAssociatorSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "associate" should {

    "succeed if finding the Project Path, token encryption and storing in the db is successful" in new TestCase {
      val projectPath = projectPaths.generateOne
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(Some(projectPath).pure[IO])

      val encryptedAccessToken = encryptedAccessTokens.generateOne
      (accessTokenCrypto
        .encrypt(_: AccessToken))
        .expects(accessToken)
        .returning(encryptedAccessToken.pure[IO])

      (associationPersister
        .persistAssociation(_: Id, _: Path, _: EncryptedAccessToken))
        .expects(projectId, projectPath, encryptedAccessToken)
        .returning(IO.unit)

      tokenAssociator.associate(projectId, accessToken).unsafeRunSync() shouldBe ()
    }

    "succeed if finding the Project Path returns none and removing the token is successful" in new TestCase {
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(Option.empty[Path].pure[IO])

      (tokenRemover
        .delete(_: Id))
        .expects(projectId)
        .returning(IO.unit)

      tokenAssociator.associate(projectId, accessToken).unsafeRunSync() shouldBe ()
    }

    "fail if finding Project Path fails" in new TestCase {
      val exception = exceptions.generateOne
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(exception.raiseError[IO, Option[Path]])

      intercept[Exception] {
        tokenAssociator.associate(projectId, accessToken).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
    }

    "fail if token encryption fails" in new TestCase {
      val projectPath = projectPaths.generateOne
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(Some(projectPath).pure[IO])

      val exception = exceptions.generateOne
      (accessTokenCrypto
        .encrypt(_: AccessToken))
        .expects(accessToken)
        .returning(exception.raiseError[IO, EncryptedAccessToken])

      intercept[Exception] {
        tokenAssociator.associate(projectId, accessToken).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
    }

    "fail if storing in the db fails" in new TestCase {
      val projectPath = projectPaths.generateOne
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(Some(projectPath).pure[IO])

      val encryptedAccessToken = encryptedAccessTokens.generateOne
      (accessTokenCrypto
        .encrypt(_: AccessToken))
        .expects(accessToken)
        .returning(encryptedAccessToken.pure[IO])

      val exception = exceptions.generateOne
      (associationPersister
        .persistAssociation(_: Id, _: Path, _: EncryptedAccessToken))
        .expects(projectId, projectPath, encryptedAccessToken)
        .returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        tokenAssociator.associate(projectId, accessToken).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
    }

    "fail if removing the token fails" in new TestCase {
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(Option.empty[Path].pure[IO])

      val exception = exceptions.generateOne
      (tokenRemover
        .delete(_: Id))
        .expects(projectId)
        .returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        tokenAssociator.associate(projectId, accessToken).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
    }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val accessToken = accessTokens.generateOne

    val projectPathFinder    = mock[ProjectPathFinder[IO]]
    val accessTokenCrypto    = mock[AccessTokenCrypto[IO]]
    val associationPersister = mock[AssociationPersister[IO]]
    val tokenRemover         = mock[TokenRemover[IO]]
    val tokenAssociator =
      new TokenAssociatorImpl[IO](projectPathFinder, accessTokenCrypto, associationPersister, tokenRemover)
  }
}
