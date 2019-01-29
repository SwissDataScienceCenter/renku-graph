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

package ch.datascience.webhookservice.hookcreation

import cats._
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators.projectIds
import ch.datascience.graph.events.ProjectId
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.crypto.HookTokenCrypto.{HookAuthToken, Secret}
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.model.AccessToken
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.language.higherKinds
import scala.util.Try

class HookCreatorSpec extends WordSpec with MockFactory {

  "create" should {

    "log success and return right if creation of the hook in GitLab was successful" in new TestCase {

      (hookTokenCrypto
        .encrypt(_: String))
        .expects(projectId.toString)
        .returning(context.pure(hookAuthToken))

      (gitLabHookCreation
        .createHook(_: ProjectId, _: AccessToken, _: HookAuthToken))
        .expects(projectId, accessToken, hookAuthToken)
        .returning(context.pure(()))

      hookCreation.createHook(projectId, accessToken) shouldBe context.pure(())

      logger.loggedOnly(Info, s"Hook created for project with id $projectId")
    }

    "log an error and return left if encryption of the hook auth token was unsuccessful" in new TestCase {

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (hookTokenCrypto
        .encrypt(_: String))
        .expects(projectId.toString)
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error, s"Hook creation failed for project with id $projectId", exception)
    }

    "log an error and return left if creation of the hook in GitLab was unsuccessful" in new TestCase {

      (hookTokenCrypto
        .encrypt(_: String))
        .expects(projectId.toString)
        .returning(context.pure(hookAuthToken))

      val exception: Exception    = exceptions.generateOne
      val error:     Try[Nothing] = context.raiseError(exception)
      (gitLabHookCreation
        .createHook(_: ProjectId, _: AccessToken, _: HookAuthToken))
        .expects(projectId, accessToken, hookAuthToken)
        .returning(error)

      hookCreation.createHook(projectId, accessToken) shouldBe error

      logger.loggedOnly(Error, s"Hook creation failed for project with id $projectId", exception)
    }
  }

  private trait TestCase {
    val projectId     = projectIds.generateOne
    val accessToken   = accessTokens.generateOne
    val hookAuthToken = hookAuthTokens.generateOne

    val context = MonadError[Try, Throwable]

    val logger             = TestLogger[Try]()
    val gitLabHookCreation = mock[HookCreationRequestSender[Try]]

    class TryHookTokenCrypt(secret: Secret) extends HookTokenCrypto[Try](secret)
    val hookTokenCrypto = mock[TryHookTokenCrypt]

    val hookCreation = new HookCreator[Try](gitLabHookCreation, logger, hookTokenCrypto)
  }
}
