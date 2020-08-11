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

package ch.datascience.tokenrepository.repository.association

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.http.client.AccessToken
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.RepositoryGenerators._
import ch.datascience.tokenrepository.repository.TryAccessTokenCrypto
import ch.datascience.tokenrepository.repository.deletion.TryTokenRemover
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers._
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TokenAssociatorSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "associate" should {

    "succeed if finding the Project Path, token encryption and storing in the db is successful" in new TestCase {
      val projectPath = projectPaths.generateOne
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(Some(projectPath)))

      val encryptedAccessToken = encryptedAccessTokens.generateOne
      (accessTokenCrypto
        .encrypt(_: AccessToken))
        .expects(accessToken)
        .returning(context.pure(encryptedAccessToken))

      (associationPersister
        .persistAssociation(_: Id, _: Path, _: EncryptedAccessToken))
        .expects(projectId, projectPath, encryptedAccessToken)
        .returning(context.unit)

      tokenAssociator.associate(projectId, accessToken) shouldBe context.unit
    }

    "succeed if finding the Project Path returns none and removing the token is successful" in new TestCase {
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(None))

      (tokenRemover
        .delete(_: Id))
        .expects(projectId)
        .returning(context.unit)

      tokenAssociator.associate(projectId, accessToken) shouldBe context.unit
    }

    "fail if finding Project Path fails" in new TestCase {
      val exception = exceptions.generateOne
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context raiseError exception)

      tokenAssociator.associate(projectId, accessToken) shouldBe context.raiseError(exception)
    }

    "fail if token encryption fails" in new TestCase {
      val projectPath = projectPaths.generateOne
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(Some(projectPath)))

      val exception = exceptions.generateOne
      (accessTokenCrypto
        .encrypt(_: AccessToken))
        .expects(accessToken)
        .returning(context.raiseError(exception))

      tokenAssociator.associate(projectId, accessToken) shouldBe context.raiseError(exception)
    }

    "fail if storing in the db fails" in new TestCase {
      val projectPath = projectPaths.generateOne
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(Some(projectPath)))

      val encryptedAccessToken = encryptedAccessTokens.generateOne
      (accessTokenCrypto
        .encrypt(_: AccessToken))
        .expects(accessToken)
        .returning(context.pure(encryptedAccessToken))

      val exception = exceptions.generateOne
      (associationPersister
        .persistAssociation(_: Id, _: Path, _: EncryptedAccessToken))
        .expects(projectId, projectPath, encryptedAccessToken)
        .returning(context.raiseError(exception))

      tokenAssociator.associate(projectId, accessToken) shouldBe context.raiseError(exception)
    }

    "fail if removing the token fails" in new TestCase {
      (projectPathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(accessToken))
        .returning(context.pure(None))

      val exception = exceptions.generateOne
      (tokenRemover
        .delete(_: Id))
        .expects(projectId)
        .returning(context raiseError exception)

      tokenAssociator.associate(projectId, accessToken) shouldBe context.raiseError(exception)
    }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val accessToken = accessTokens.generateOne

    val context = MonadError[Try, Throwable]

    val projectPathFinder    = mock[ProjectPathFinder[Try]]
    val accessTokenCrypto    = mock[TryAccessTokenCrypto]
    val associationPersister = mock[TryAssociationPersister]
    val tokenRemover         = mock[TryTokenRemover]
    val tokenAssociator = new TokenAssociator[Try](
      projectPathFinder,
      accessTokenCrypto,
      associationPersister,
      tokenRemover
    )
  }
}
