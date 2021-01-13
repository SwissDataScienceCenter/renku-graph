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

package ch.datascience.triplesgenerator
package events.awaitinggeneration

import cats.MonadError
import cats.data.{EitherT, NonEmptyList}
import cats.effect.{ContextShift, Effect, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.graph.model.events.{CompoundEventId, EventBody, EventId}
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.EventHandler.CategoryName
import ch.datascience.triplesgenerator.events.EventSchedulingResult
import ch.datascience.triplesgenerator.events.EventSchedulingResult._
import ch.datascience.triplesgenerator.subscriptions.Subscriber
import io.chrisdavenport.log4cats.Logger
import io.circe.DecodingFailure

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[events] class EventHandler[Interpretation[_]: Effect](
    eventsProcessingRunner: EventsProcessingRunner[Interpretation],
    eventBodyDeserializer:  EventBodyDeserializer[Interpretation],
    currentVersionPair:     RenkuVersionPair,
    logger:                 Logger[Interpretation]
)(implicit
    ME: MonadError[Interpretation, Throwable]
) extends events.EventHandler[Interpretation] {

  import cats.syntax.all._
  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import currentVersionPair.schemaVersion
  import eventBodyDeserializer.toCommitEvents
  import eventsProcessingRunner.scheduleForProcessing
  import io.circe.{Decoder, HCursor}
  import org.http4s._
  import org.http4s.circe._

  private type IdAndBody = (CompoundEventId, EventBody)

  override val name: CategoryName = CategoryName("AWAITING_GENERATION")

  override def handle(request: Request[Interpretation]): Interpretation[EventSchedulingResult] = {
    for {
      eventAndBody <- request.as[IdAndBody].toRightT(recoverTo = UnsupportedEventType)
      (eventId, eventBody) = eventAndBody
      commitEvents <- toCommitEvents(eventBody).toRightT(recoverTo = BadRequest)
      result <- scheduleForProcessing(eventId, commitEvents, schemaVersion).toRightT(
                  recoverTo = SchedulingError,
                  maybeEventInfo = (eventId -> commitEvents.head.project.path).some
                )
      _ = logAccepted(eventId, commitEvents, result)
    } yield result
  }.merge

  private implicit class EitherTOps[T](operation: Interpretation[T]) {

    def toRightT(
        recoverTo:      EventSchedulingResult,
        maybeEventInfo: Option[(CompoundEventId, projects.Path)] = None
    ): EitherT[Interpretation, EventSchedulingResult, T] = EitherT {
      operation map (_.asRight[EventSchedulingResult]) recover as(recoverTo, maybeEventInfo)
    }

    private def as(
        result:         EventSchedulingResult,
        maybeEventInfo: Option[(CompoundEventId, projects.Path)]
    ): PartialFunction[Throwable, Either[EventSchedulingResult, T]] = { case NonFatal(exception) =>
      logSchedulingError(result, exception, maybeEventInfo)
      Left(result)
    }

    private def logSchedulingError(
        result:         EventSchedulingResult,
        exception:      Throwable,
        maybeEventInfo: Option[(CompoundEventId, projects.Path)]
    ) =
      result match {
        case SchedulingError =>
          maybeEventInfo map { case (eventId, projectPath) =>
            logger.error(exception)(s"$name: $eventId, projectPath = $projectPath -> $result")
          }
        case _ => ME.unit
      }
  }

  private def logAccepted(eventId:      CompoundEventId,
                          commitEvents: NonEmptyList[CommitEvent],
                          result:       EventSchedulingResult
  ): Interpretation[Unit] = result match {
    case Accepted => logger.info(s"$name: $eventId, projectPath = ${commitEvents.head.project.path} -> $result")
    case _        => ME.unit
  }

  private implicit lazy val payloadDecoder: EntityDecoder[Interpretation, IdAndBody] = jsonOf[Interpretation, IdAndBody]

  private implicit val eventDecoder: Decoder[IdAndBody] = (cursor: HCursor) =>
    for {
      _         <- cursor.downField("categoryName").as[CategoryName] flatMap checkCategoryName
      id        <- cursor.downField("id").as[EventId]
      projectId <- cursor.downField("project").downField("id").as[projects.Id]
      body      <- cursor.downField("body").as[EventBody]
    } yield CompoundEventId(id, projectId) -> body

  private lazy val checkCategoryName: CategoryName => Decoder.Result[CategoryName] = {
    case name @ `name` => Right(name)
    case other         => Left(DecodingFailure(s"$other not supported by $name", Nil))
  }
}

private[events] object EventHandler {

  def apply(
      currentVersionPair: RenkuVersionPair,
      metricsRegistry:    MetricsRegistry[IO],
      gitLabThrottler:    Throttler[IO, GitLab],
      timeRecorder:       SparqlQueryTimeRecorder[IO],
      subscriber:         Subscriber[IO],
      logger:             Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    processingRunner <- IOEventsProcessingRunner(metricsRegistry, gitLabThrottler, timeRecorder, subscriber, logger)
  } yield new EventHandler[IO](processingRunner, EventBodyDeserializer(), currentVersionPair, logger)
}
