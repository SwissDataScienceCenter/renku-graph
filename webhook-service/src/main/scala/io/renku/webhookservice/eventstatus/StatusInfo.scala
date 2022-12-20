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

import io.circe.Encoder
import io.circe.literal._
import io.renku.graph.model.events.{EventStatus, EventStatusProgress}

private sealed trait StatusInfo {
  val activated: Boolean
  val progress:  ProgressStatus
}

private object StatusInfo {

  final case class ActivatedProject(progress: ProgressStatus.NonZero) extends StatusInfo {
    override val activated: Boolean = true
  }

  def activated(eventStatus: EventStatus): StatusInfo.ActivatedProject =
    ActivatedProject(ProgressStatus.from(eventStatus))

  final case object NotActivated extends StatusInfo {
    override val activated: Boolean        = false
    override val progress:  ProgressStatus = ProgressStatus.Zero
  }

  implicit def encoder[PS <: StatusInfo]: Encoder[PS] = {
    case ActivatedProject(status: ProgressStatus.NonZero) => json"""{
        "activated": true,
        "done":      ${status.statusProgress.stage.value},
        "total":     ${EventStatusProgress.Stage.Final.value},
        "progress":  ${status.statusProgress.completion.value}
      }"""
    case NotActivated => json"""{
        "activated": false,
        "done":      0,
        "total":     0
      }"""
  }
}

private sealed trait ProgressStatus extends Product with Serializable

private object ProgressStatus {

  final case object Zero extends ProgressStatus

  final case class NonZero(statusProgress: EventStatusProgress) extends ProgressStatus {
    lazy val currentStage: EventStatusProgress.Stage      = statusProgress.stage
    lazy val finalStage:   EventStatusProgress.Stage      = EventStatusProgress.Stage.Final
    lazy val completion:   EventStatusProgress.Completion = statusProgress.completion
  }

  def from(eventStatus: EventStatus): ProgressStatus.NonZero =
    ProgressStatus.NonZero(EventStatusProgress(eventStatus))
}
