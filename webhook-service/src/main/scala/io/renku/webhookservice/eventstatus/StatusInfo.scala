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

package io.renku.webhookservice.eventstatus

import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.EventStatusProgress.Stage
import io.renku.graph.model.events.{EventInfo, EventStatus, EventStatusProgress}

private sealed trait StatusInfo extends Product {
  def activated: Boolean
  def progress:  Progress

  def fold[A](activated: StatusInfo.ActivatedProject => A, whenNotActivated: => A): A

  lazy val widen: StatusInfo = this
}

private object StatusInfo {

  final case class ActivatedProject(progress: Progress, details: Details) extends StatusInfo {
    override val activated: Boolean = true
    def fold[A](whenActivated: StatusInfo.ActivatedProject => A, notActivated: => A): A = whenActivated(this)
  }

  def activated(event: EventInfo): StatusInfo.ActivatedProject =
    ActivatedProject(Progress.from(event.status), Details.from(event))

  def webhookReady: StatusInfo.ActivatedProject =
    ActivatedProject(Progress.Zero, Details("in-progress", "Webhook has been installed.", maybeDetails = None))

  final case object NotActivated extends StatusInfo {
    override val activated: Boolean  = false
    override val progress:  Progress = Progress.Zero
    def fold[A](whenActivated: StatusInfo.ActivatedProject => A, whenNotActivated: => A): A = whenNotActivated
  }

  implicit def encoder[PS <: StatusInfo]: Encoder[PS] =
    Encoder.instance { statusInfo =>
      Json
        .obj(
          "activated" -> statusInfo.activated.asJson,
          "progress"  -> statusInfo.progress.asJson,
          "details"   -> statusInfo.fold(_.details.some, None).asJson
        )
        .deepDropNullValues
    }
}

private sealed trait Progress extends Product {
  final val total: Int = Stage.Final.value
  def done:        Int
  def percentage:  Float
}

private object Progress {

  final case object Zero extends Progress {
    val done       = 0
    val percentage = 0f
  }

  final case class NonZero(statusProgress: EventStatusProgress) extends Progress {
    lazy val currentStage: EventStatusProgress.Stage      = statusProgress.stage
    lazy val completion:   EventStatusProgress.Completion = statusProgress.completion

    lazy val done:       Int   = currentStage.value
    lazy val percentage: Float = completion.value
  }

  def from(eventStatus: EventStatus): Progress.NonZero =
    Progress.NonZero(EventStatusProgress(eventStatus))

  implicit val jsonEncoder: Encoder[Progress] = Encoder.instance { progress =>
    Json.obj(
      "done"       -> progress.done.asJson,
      "total"      -> progress.total.asJson,
      "percentage" -> progress.percentage.asJson
    )
  }
}

private final case class Details(status: String, message: String, maybeDetails: Option[String])

private object Details {

  def from(event: EventInfo): Details = {

    val status: String = event.status match {
      case New | GeneratingTriples | GenerationRecoverableFailure | TriplesGenerated | TransformingTriples |
          TransformationRecoverableFailure | AwaitingDeletion | Deleting =>
        "in-progress"
      case Skipped | TriplesStore                                                => "success"
      case GenerationNonRecoverableFailure | TransformationNonRecoverableFailure => "failure"
    }

    val message: String = event.status.show.toLowerCase.replace('_', ' ')

    Details(status, message, event.maybeMessage.map(_.value))
  }

  implicit val jsonEncoder: Encoder[Details] =
    Encoder.instance { d =>
      Json
        .obj("status" -> d.status.asJson, "message" -> d.message.asJson, "stacktrace" -> d.maybeDetails.asJson)
        .deepDropNullValues
    }
}
