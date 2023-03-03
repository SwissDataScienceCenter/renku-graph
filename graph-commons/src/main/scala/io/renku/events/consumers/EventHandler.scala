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

package io.renku.events.consumers

import cats.effect.{MonadCancelThrow, Resource}
import cats.syntax.all._
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.events.consumers.EventSchedulingResult._
import org.typelevel.log4cats.Logger

trait EventHandler[F[_]] {

  val categoryName: CategoryName

  def tryHandling(request: EventRequestContent): F[EventSchedulingResult]

  import EventDecodingTools._

  protected lazy val checkCategory: EventRequestContent => Either[EventSchedulingResult, CategoryName] =
    _.event.categoryName.leftMap(_ => UnsupportedEventType.widen) >>= checkCategoryName

  private lazy val checkCategoryName: CategoryName => Either[EventSchedulingResult, CategoryName] = {
    case name @ `categoryName` => name.asRight
    case _                     => UnsupportedEventType.asLeft
  }
}

abstract class EventHandlerWithProcessLimiter[F[_]: MonadCancelThrow: Logger](
    processExecutor: ProcessExecutor[F]
) extends EventHandler[F] {

  protected type Event
  protected def createHandlingDefinition(): EventHandlingDefinition

  @annotation.nowarn("cat=unused")
  protected def onPostHandling(event: Event, result: EventSchedulingResult): F[Unit] = ().pure[F]

  final override def tryHandling(request: EventRequestContent): F[EventSchedulingResult] =
    checkCategory(request)
      .leftMap(_.pure[F])
      .as(doCategoryHandling(request))
      .merge

  private def doCategoryHandling(request: EventRequestContent) = {

    val handlingDefinition = createHandlingDefinition()

    handlingDefinition.precondition >>= {
      case Some(preconditionFailure) => preconditionFailure.pure[F]
      case None =>
        decodeEvent(request, handlingDefinition)
          .map(event => process(event, handlingDefinition).flatTap(result => onPostHandling(event, result)))
          .sequence
          .map(_.merge)
    }
  }

  private def decodeEvent(request:           EventRequestContent,
                          processDefinition: EventHandlingDefinition
  ): Either[EventSchedulingResult, Event] =
    (processDefinition decode request).leftMap(err => BadRequest(err.getMessage))

  private def process(event: Event, processDefinition: EventHandlingDefinition): F[EventSchedulingResult] = {
    val processResource = Resource
      .make(().pure[F])(_ => processDefinition.onRelease.getOrElse(().pure[F]))
      .evalTap(_ => processDefinition.process(event))
    processExecutor
      .tryExecuting(processResource.use_)
  }

  case class EventHandlingDefinition(
      decode:       EventRequestContent => Either[Exception, Event],
      process:      Event => F[Unit],
      precondition: F[Option[EventSchedulingResult]],
      onRelease:    Option[F[Unit]] = None
  )

  object EventHandlingDefinition {

    def apply(decode:    EventRequestContent => Either[Exception, Event],
              process:   Event => F[Unit],
              onRelease: F[Unit]
    ): EventHandlingDefinition = EventHandlingDefinition(
      decode,
      process,
      precondition = Option.empty[EventSchedulingResult].pure[F],
      onRelease.some
    )

    def apply(decode:  EventRequestContent => Either[Exception, Event],
              process: Event => F[Unit]
    ): EventHandlingDefinition = EventHandlingDefinition(
      decode,
      process,
      precondition = Option.empty[EventSchedulingResult].pure[F],
      onRelease = None
    )
  }
}
