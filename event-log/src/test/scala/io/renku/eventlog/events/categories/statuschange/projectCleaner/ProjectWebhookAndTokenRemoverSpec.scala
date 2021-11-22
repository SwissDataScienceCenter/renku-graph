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

package io.renku.eventlog.events.categories.statuschange.projectCleaner

import cats.effect.IO
import cats.implicits.toShow
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, delete, stubFor}
import io.renku.eventlog.events.categories.statuschange.Generators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.httpUrls
import io.renku.graph.tokenrepository.TokenRepositoryUrl
import io.renku.graph.webhookservice.WebhookServiceUrl
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status.{InternalServerError, NoContent, Ok, Unauthorized}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectWebhookAndTokenRemoverSpec
    extends AnyWordSpec
    with ExternalServiceStubbing
    with IOSpec
    with should.Matchers {
  "removeWebhookAndToken" should {
    "remove the token and the web hook of the specified project" in new TestCase {
      stubFor {
        delete(s"$webhookServiceUrl/projects/${project.id}/webhooks")
          .willReturn(aResponse().withStatus(Ok.code))
      }
      stubFor {
        delete(s"$tokenRepositoryUrl/projects/${project.id}/tokens")
          .willReturn(aResponse().withStatus(NoContent.code))
      }

      webhookAndTokenRemover.removeWebhookAndToken(project).unsafeRunSync() shouldBe ()
    }
    "fail when the webhook deletion fails" in new TestCase {
      stubFor {
        delete(s"$webhookServiceUrl/projects/${project.id}/webhooks")
          .willReturn(aResponse().withStatus(Unauthorized.code))
      }
      intercept[Exception] {
        webhookAndTokenRemover.removeWebhookAndToken(project).unsafeRunSync()
      }.getMessage shouldBe s"Removing project webhook failed with status: $Unauthorized for project: ${project.show}"
    }
    "fail when the token deletion fails" in new TestCase {
      stubFor {
        delete(s"$webhookServiceUrl/projects/${project.id}/webhooks")
          .willReturn(aResponse().withStatus(Ok.code))
      }
      stubFor {
        delete(s"$tokenRepositoryUrl/projects/${project.id}/tokens")
          .willReturn(aResponse().withStatus(InternalServerError.code))
      }
      intercept[Exception] {
        webhookAndTokenRemover.removeWebhookAndToken(project).unsafeRunSync()
      }.getMessage shouldBe s"Removing project token failed with status: $Unauthorized for project: ${project.show}"
    }
  }

  private trait TestCase {
    val project                = consumerProjects.generateOne
    implicit val logger        = TestLogger[IO]()
    val tokenRepositoryUrl     = httpUrls().generateAs(TokenRepositoryUrl)
    val webhookServiceUrl      = httpUrls().generateAs(WebhookServiceUrl)
    val webhookAndTokenRemover = new ProjectWebhookAndTokenRemoverImpl[IO](webhookServiceUrl, tokenRepositoryUrl)
  }
}
