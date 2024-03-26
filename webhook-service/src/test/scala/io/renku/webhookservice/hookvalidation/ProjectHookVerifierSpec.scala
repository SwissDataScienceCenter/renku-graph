/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.webhookservice.hookvalidation

import cats.effect.IO
import cats.syntax.all._
import io.renku.http.client.GitLabGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectHookVerifierSpec
    extends AnyWordSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers
    with IOSpec {

  "checkHookPresence" should {

    "return true if there's a hook with url pointing to expected project hook url" in new TestCase {

      val idsAndUrls =
        hookIdAndUrls.generateList() :+ HookIdAndUrl(positiveInts().generateOne.value, projectHookId.projectHookUrl)
      val accessToken = accessTokens.generateOne
      (projectHookFetcher.fetchProjectHooks _).expects(projectId, accessToken).returns(idsAndUrls.some.pure[IO])

      verifier.checkHookPresence(projectHookId, accessToken).unsafeRunSync() shouldBe true.some
    }

    "return false if there's no hook with url pointing to expected project hook url" in new TestCase {

      val idsAndUrls  = hookIdAndUrls.generateNonEmptyList().toList
      val accessToken = accessTokens.generateOne
      (projectHookFetcher.fetchProjectHooks _).expects(projectId, accessToken).returns(idsAndUrls.some.pure[IO])

      verifier.checkHookPresence(projectHookId, accessToken).unsafeRunSync() shouldBe false.some
    }

    "return None if finding hooks returns None" in new TestCase {

      val accessToken = accessTokens.generateOne
      (projectHookFetcher.fetchProjectHooks _)
        .expects(projectId, accessToken)
        .returns(None.pure[IO])

      verifier.checkHookPresence(projectHookId, accessToken).unsafeRunSync() shouldBe None
    }

    "fail if finding hooks fails" in new TestCase {

      val exception   = exceptions.generateOne
      val accessToken = accessTokens.generateOne
      (projectHookFetcher.fetchProjectHooks _)
        .expects(projectId, accessToken)
        .returns(exception.raiseError[IO, Nothing])

      intercept[Exception] {
        verifier.checkHookPresence(projectHookId, accessToken).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val projectHookId      = projectHookIds.generateOne
    val projectId          = projectHookId.projectId
    val projectHookFetcher = mock[ProjectHookFetcher[IO]]
    val verifier           = new ProjectHookVerifierImpl[IO](projectHookFetcher)
  }
}
