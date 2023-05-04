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
import io.renku.graph.model.EventContentGenerators.{eventInfos, eventMessages}
import io.renku.graph.model.EventsGenerators.eventStatuses
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{EventMessage, EventStatus, EventStatusProgress}
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StatusInfoSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "encode" should {

    "produce Json from a StatusInfo of an activated project with a NonZero progress" in {

      forAll(eventInfos()) { eventInfo =>
        val info = StatusInfo.activated(eventInfo)

        info.asJson shouldBe json"""{
          "activated": true,
          "progress": {
            "done":       ${info.progress.done},
            "total":      ${info.progress.total},
            "percentage": ${info.progress.percentage}
          },
          "details": {
             "status":     ${info.details.status},
             "message":    ${info.details.message},
             "stacktrace": ${info.details.maybeDetails}
          }
        }""".deepDropNullValues
      }
    }

    "produce Json from a StatusInfo of a non-activated project" in {

      val info = StatusInfo.NotActivated

      info.asJson shouldBe json"""{
        "activated": false,
        "progress": {
          "done":       0,
          "total":      ${info.progress.total},
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
        progressStatus.total        shouldBe EventStatusProgress.Stage.Final.value
        progressStatus.completion   shouldBe EventStatusProgress(eventStatus).completion
      }
    }
  }

  "Progress.Zero to have final stage set to EventStatusProgress.Stage.Final" in {
    Progress.Zero.total shouldBe EventStatusProgress.Stage.Final.value
  }
}

class DetailsSpec extends AnyWordSpec with should.Matchers with TableDrivenPropertyChecks {

  "Details" should {

    forAll(
      Table(
        ("eventStatus", "status", "message", "maybeMessage"),
        (New, "in-progress", "new", Option.empty[EventMessage]),
        (Skipped, "success", "skipped", eventMessages.generateSome),
        (GeneratingTriples, "in-progress", "generating triples", Option.empty[EventMessage]),
        (GenerationRecoverableFailure, "in-progress", "generation recoverable failure", eventMessages.generateSome),
        (GenerationNonRecoverableFailure, "failure", "generation non recoverable failure", eventMessages.generateSome),
        (TriplesGenerated, "in-progress", "triples generated", Option.empty[EventMessage]),
        (TransformingTriples, "in-progress", "transforming triples", Option.empty[EventMessage]),
        (TransformationRecoverableFailure,
         "in-progress",
         "transformation recoverable failure",
         eventMessages.generateSome
        ),
        (TransformationNonRecoverableFailure,
         "failure",
         "transformation non recoverable failure",
         eventMessages.generateSome
        ),
        (TriplesStore, "success", "triples store", Option.empty[EventMessage]),
        (AwaitingDeletion, "in-progress", "awaiting deletion", Option.empty[EventMessage]),
        (Deleting, "in-progress", "deleting", Option.empty[EventMessage])
      )
    ) { (eventStatus, status, message, maybeEventMessage) =>
      show"provide '$status' as status and '$message' as message for the '$eventStatus' status" in {
        val details =
          Details.from(eventInfos().generateOne.copy(status = eventStatus, maybeMessage = maybeEventMessage))

        details.status       shouldBe status
        details.message      shouldBe message
        details.maybeDetails shouldBe maybeEventMessage.map(_.value)
      }
    }
  }
}
