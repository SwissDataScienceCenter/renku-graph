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

package io.renku.eventlog.events.consumers.statuschange
package redoprojecttransformation

import cats.Show
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure, Encoder}
import io.circe.literal._
import io.renku.events.EventRequestContent
import io.renku.graph.model.{events, projects}
import io.renku.graph.model.events.EventStatus._
import io.renku.tinytypes.json.TinyTypeDecoders._

private final case class RedoProjectTransformation(projectPath: projects.Path) extends StatusChangeEvent {
  override val silent: Boolean = false
}

private object RedoProjectTransformation {

  val decoder: EventRequestContent => Either[DecodingFailure, RedoProjectTransformation] = { request =>
    for {
      projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
      _ <- request.event.hcursor.downField("newStatus").as[events.EventStatus] >>= {
             case TriplesGenerated => Right(())
             case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield RedoProjectTransformation(projectPath)
  }

  implicit lazy val encoder: Encoder[RedoProjectTransformation] = Encoder.instance {
    case RedoProjectTransformation(path) => json"""{
      "project": {
        "path": ${path.value}
      }
    }"""
  }

  implicit lazy val eventDecoder: Decoder[RedoProjectTransformation] =
    _.downField("project").downField("path").as[projects.Path].map(RedoProjectTransformation(_))

  implicit lazy val eventType: StatusChangeEventsQueue.EventType[RedoProjectTransformation] =
    StatusChangeEventsQueue.EventType("REDO_PROJECT_TRANSFORMATION")

  implicit lazy val show: Show[RedoProjectTransformation] = Show.show { case RedoProjectTransformation(projectPath) =>
    s"projectPath = $projectPath, status = $TriplesGenerated - redo"
  }
}
