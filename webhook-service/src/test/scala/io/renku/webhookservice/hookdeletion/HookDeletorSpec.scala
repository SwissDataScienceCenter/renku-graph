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

package io.renku.webhookservice.hookdeletion

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.projects.Id
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import io.renku.webhookservice.WebhookServiceGenerators.{hookIdAndUrls, projectHookIds}
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult.{HookDeleted, HookNotFound}
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class HookDeletorSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "deleteHook" should {

    "return HookeDeleted if hook exists and it was successfully deleted" in new TestCase {
      val hookToDelete = hookIdAndUrls.generateOne.copy(url = projectHookId.projectHookUrl)
      val idsAndUrls   = hookIdAndUrls.generateNonEmptyList().toList :+ hookToDelete

      (projectHookFetcher.fetchProjectHooks _)
        .expects(projectHookId.projectId, accessToken)
        .returns(idsAndUrls.pure[IO])

      (projectHookDeletor
        .delete(_: Id, _: HookIdAndUrl, _: AccessToken))
        .expects(projectHookId.projectId, hookToDelete, accessToken)
        .returning(HookDeleted.pure[IO])

      hookDeletor.deleteHook(projectHookId, accessToken).unsafeRunSync() shouldBe HookDeleted

      logger.expectNoLogs()
    }

    "return HookNotFound if hook was already removed for that project" in new TestCase {
      val idsAndUrls = hookIdAndUrls.generateNonEmptyList().toList

      (projectHookFetcher.fetchProjectHooks _)
        .expects(projectHookId.projectId, accessToken)
        .returns(idsAndUrls.pure[IO])

      hookDeletor.deleteHook(projectHookId, accessToken).unsafeRunSync() shouldBe HookNotFound

      logger.expectNoLogs()
    }

    "log an error if hook deletion fails" in new TestCase {
      val hookToDelete = hookIdAndUrls.generateOne.copy(url = projectHookId.projectHookUrl)
      val idsAndUrls   = hookIdAndUrls.generateNonEmptyList().toList :+ hookToDelete

      (projectHookFetcher.fetchProjectHooks _)
        .expects(projectHookId.projectId, accessToken)
        .returns(idsAndUrls.pure[IO])

      val exception = exceptions.generateOne
      (projectHookDeletor
        .delete(_: Id, _: HookIdAndUrl, _: AccessToken))
        .expects(projectHookId.projectId, hookToDelete, accessToken)
        .returning(exception.raiseError[IO, DeletionResult])

      intercept[Exception] {
        hookDeletor.deleteHook(projectHookId, accessToken).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error(s"Hook deletion failed for project with id ${projectHookId.projectId}", exception))
    }
  }

  private trait TestCase {
    val projectHookId = projectHookIds.generateOne
    val accessToken   = accessTokens.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val projectHookFetcher = mock[ProjectHookFetcher[IO]]
    val projectHookDeletor = mock[ProjectHookDeletor[IO]]

    val hookDeletor = new HookDeletorImpl[IO](projectHookFetcher, projectHookDeletor)
  }
}
