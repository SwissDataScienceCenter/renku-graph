/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.eventStatuses
import io.renku.graph.model.events.{EventStatus, EventStatusProgress}
import EventStatus._
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StatusInfoSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "encode" should {

    "produce Json from a StatusInfo of an activated project with a NonZero progress" in {
      forAll { eventStatus: EventStatus =>
        val info = StatusInfo.activated(eventStatus)

        info.asJson shouldBe json"""{
          "activated": true,
          "progress": {
            "done":       ${info.progress.currentStage.value},
            "total":      ${info.progress.finalStage.value},
            "percentage": ${info.progress.completion.value}
          },
          "details": {
             "status":  ${info.details.status},
             "message": ${info.details.message}
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
          "total":      ${info.progress.finalStage.value},
          "percentage": 0.00
        }
      }"""
    }
  }
}

class ProgressSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "from" should {

    "return Progress.NonZero for the given EventStatus" in {
      forAll { (eventStatus: EventStatus) =>
        val progressStatus = Progress.from(eventStatus)

        progressStatus.currentStage shouldBe EventStatusProgress(eventStatus).stage
        progressStatus.finalStage   shouldBe EventStatusProgress.Stage.Final
        progressStatus.completion   shouldBe EventStatusProgress(eventStatus).completion
      }
    }
  }

  "Progress.Zero to have final stage set to EventStatusProgress.Stage.Final" in {
    Progress.Zero.finalStage shouldBe EventStatusProgress.Stage.Final
  }
}

class DetailsSpec extends AnyWordSpec with should.Matchers with TableDrivenPropertyChecks {

  "Details" should {

    forAll(
      Table(
        ("eventStatus", "status", "message"),
        (New, "in-progress", "new"),
        (Skipped, "success", "skipped"),
        (GeneratingTriples, "in-progress", "generating triples"),
        (GenerationRecoverableFailure, "in-progress", "generation recoverable failure"),
        (GenerationNonRecoverableFailure, "failure", "generation non recoverable failure"),
        (TriplesGenerated, "in-progress", "triples generated"),
        (TransformingTriples, "in-progress", "transforming triples"),
        (TransformationRecoverableFailure, "in-progress", "transformation recoverable failure"),
        (TransformationNonRecoverableFailure, "failure", "transformation non recoverable failure"),
        (TriplesStore, "success", "triples store"),
        (AwaitingDeletion, "in-progress", "awaiting deletion"),
        (Deleting, "in-progress", "deleting")
      )
    ) { (eventStatus, status, message) =>
      show"provide '$status' as status and '$message' as message for the '$eventStatus' status" in {
        val details = Details(eventStatus)

        details.status  shouldBe status
        details.message shouldBe message
      }
    }
  }
}
