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

package ch.datascience.webhookservice.project

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.webhookservice.eventprocessing.routes.WebhookEventEndpoint
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.project.ProjectHookUrlFinder.ProjectHookUrl
import ch.datascience.webhookservice.project.SelfUrlConfig.SelfUrl
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import play.api.Configuration

import scala.util.{Failure, Success, Try}

class ProjectHookUrlFinderSpec extends WordSpec with MockFactory {

  "findProjectHookUrl" should {

    s"return project hook url composed of selfUrl and ${WebhookEventEndpoint.processPushEvent()} " +
      "if selfUrl can be obtained" in new TestCase {
      expectSelfUrlConfig(returning = context.pure(selfUrl))

      hookUrlFinder.findProjectHookUrl shouldBe Success(
        ProjectHookUrl(
          s"$selfUrl${WebhookEventEndpoint.processPushEvent().url}"
        )
      )
    }

    "fail if finding selfUrl fails" in new TestCase {
      val exception = exceptions.generateOne
      expectSelfUrlConfig(returning = context.raiseError(exception))

      hookUrlFinder.findProjectHookUrl shouldBe Failure(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val selfUrl = selfUrls.generateOne

    def expectSelfUrlConfig(returning: Try[SelfUrl]) =
      (selfUrlConfig.get _)
        .expects()
        .returning(returning)

    class TestSelfUrlConfig(configuration: Configuration) extends SelfUrlConfig[Try](configuration)
    val selfUrlConfig = mock[TestSelfUrlConfig]
    val hookUrlFinder = new ProjectHookUrlFinder[Try](selfUrlConfig)
  }
}

class TryProjectHookUrlFinder(selfUrlConfig: SelfUrlConfig[Try]) extends ProjectHookUrlFinder[Try](selfUrlConfig)
