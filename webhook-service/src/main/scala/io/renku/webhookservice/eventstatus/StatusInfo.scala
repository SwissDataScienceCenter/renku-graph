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

import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal._
import io.renku.graph.model.events.{EventStatus, EventStatusProgress}
import EventStatus._

private sealed trait StatusInfo {
  val activated: Boolean
  val progress:  Progress
}

private object StatusInfo {

  final case class ActivatedProject(progress: Progress.NonZero, details: Details) extends StatusInfo {
    override val activated: Boolean = true
  }

  def activated(eventStatus: EventStatus): StatusInfo.ActivatedProject =
    ActivatedProject(Progress.from(eventStatus), Details(eventStatus))

  final case object NotActivated extends StatusInfo {
    override val activated: Boolean  = false
    override val progress:  Progress = Progress.Zero
  }

  implicit def encoder[PS <: StatusInfo]: Encoder[PS] = {
    case info @ ActivatedProject(status: Progress.NonZero, details) => json"""{
      "activated": ${info.activated},
      "progress": {
        "done":       ${status.statusProgress.stage.value},
        "total":      ${EventStatusProgress.Stage.Final.value},
        "percentage": ${status.statusProgress.completion.value}
      },
      "details": {
        "status":  ${details.status},
        "message": ${details.message}
      }
    }"""
    case info @ NotActivated => json"""{
      "activated": ${info.activated},
      "progress": {
         "done":       0,
         "total":      ${EventStatusProgress.Stage.Final.value},
         "percentage": 0.00
       }
    }"""
  }
}

private sealed trait Progress extends Product with Serializable {
  lazy val finalStage: EventStatusProgress.Stage = EventStatusProgress.Stage.Final
}

private object Progress {

  final case object Zero extends Progress

  final case class NonZero(statusProgress: EventStatusProgress) extends Progress {
    lazy val currentStage: EventStatusProgress.Stage      = statusProgress.stage
    lazy val completion:   EventStatusProgress.Completion = statusProgress.completion
  }

  def from(eventStatus: EventStatus): Progress.NonZero =
    Progress.NonZero(EventStatusProgress(eventStatus))
}

private final case class Details(eventStatus: EventStatus) {

  lazy val status: String = eventStatus match {
    case New | GeneratingTriples | GenerationRecoverableFailure | TriplesGenerated | TransformingTriples |
        TransformationRecoverableFailure | AwaitingDeletion | Deleting =>
      "in-progress"
    case Skipped | TriplesStore                                                => "success"
    case GenerationNonRecoverableFailure | TransformationNonRecoverableFailure => "failure"
  }

  lazy val message: String = eventStatus.show.toLowerCase.replace('_', ' ')
}
