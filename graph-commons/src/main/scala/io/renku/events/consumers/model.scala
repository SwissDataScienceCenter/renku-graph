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

package io.renku.events.consumers

import cats.Show
import cats.data.EitherT
import cats.effect.Concurrent
import cats.effect.concurrent.Deferred
import cats.syntax.all._
import io.renku.graph.model.projects

final case class Project(id: projects.Id, path: projects.Path)

object Project {
  implicit lazy val show: Show[Project] = Show.show { case Project(id, path) =>
    s"projectId = $id, projectPath = $path"
  }
}

sealed trait EventSchedulingResult extends Product with Serializable

object EventSchedulingResult {
  type Accepted = Accepted.type
  case object Accepted                                   extends EventSchedulingResult
  case object Busy                                       extends EventSchedulingResult
  case object UnsupportedEventType                       extends EventSchedulingResult
  case object BadRequest                                 extends EventSchedulingResult
  final case class SchedulingError(throwable: Throwable) extends EventSchedulingResult

  implicit def show[SE <: EventSchedulingResult]: Show[SE] = Show.show {
    case Accepted             => "Accepted"
    case Busy                 => "Busy"
    case UnsupportedEventType => "UnsupportedEventType"
    case BadRequest           => "BadRequest"
    case SchedulingError(_)   => "SchedulingError"
  }
}

import EventSchedulingResult._

class EventHandlingProcess[Interpretation[_]: Concurrent] private (
    deferred:                Deferred[Interpretation, Unit],
    val process:             EitherT[Interpretation, EventSchedulingResult, Accepted],
    val maybeReleaseProcess: Option[Interpretation[Unit]] = None
) {
  def waitToFinish(): Interpretation[Unit] = deferred.get
}

object EventHandlingProcess {
  def withWaitingForCompletion[Interpretation[_]: Concurrent](
      process:        Deferred[Interpretation, Unit] => EitherT[Interpretation, EventSchedulingResult, Accepted],
      releaseProcess: Interpretation[Unit]
  ): Interpretation[EventHandlingProcess[Interpretation]] =
    Deferred[Interpretation, Unit].map(deferred =>
      new EventHandlingProcess[Interpretation](deferred, process(deferred), releaseProcess.some)
    )

  def apply[Interpretation[_]: Concurrent](
      process: EitherT[Interpretation, EventSchedulingResult, Accepted]
  ): Interpretation[EventHandlingProcess[Interpretation]] = for {
    deferred <- Deferred[Interpretation, Unit]
    _        <- deferred.complete(())
  } yield new EventHandlingProcess[Interpretation](deferred, process)
}
