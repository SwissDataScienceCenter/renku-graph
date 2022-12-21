/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.webhookservice.eventstatus

import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.eventStatuses
import io.renku.graph.model.events.{EventStatus, EventStatusProgress}
import io.renku.webhookservice.eventstatus.Generators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StatusInfoSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "encode" should {

    "produce Json from a StatusInfo of an activated project with a NonZero progress" in {
      forAll { progress: ProgressStatus.NonZero =>
        val info = StatusInfo.ActivatedProject(progress)

        info.asJson shouldBe json"""{
          "activated": true,
          "progress": {
            "done":       ${progress.currentStage.value},
            "total":      ${progress.finalStage.value},
            "percentage": ${progress.completion.value}
          }
        }"""
      }
    }

    "produce Json from a StatusInfo of a non-activated project" in {

      val info = StatusInfo.NotActivated

      info.asJson shouldBe json"""{
        "activated": false,
        "progress": {
          "done":       0,
          "total":      ${EventStatusProgress.Stage.Final.value},
          "percentage": 0.00
        }
      }"""
    }
  }
}

class ProgressStatusSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "from" should {

    "return ProgressStatus.NonZero for the given EventStatus" in {
      forAll { (eventStatus: EventStatus) =>
        val progressStatus = ProgressStatus.from(eventStatus)

        progressStatus.currentStage shouldBe EventStatusProgress(eventStatus).stage
        progressStatus.finalStage   shouldBe EventStatusProgress.Stage.Final
        progressStatus.completion   shouldBe EventStatusProgress(eventStatus).completion
      }
    }
  }
}
