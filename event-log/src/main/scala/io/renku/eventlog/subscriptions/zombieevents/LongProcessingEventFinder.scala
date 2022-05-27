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

package io.renku.eventlog.subscriptions.zombieevents

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import io.renku.db.implicits._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.subscriptions.EventFinder
import io.renku.eventlog.{ExecutionDate, TypeSerializers}
import io.renku.graph.model.events.EventStatus.{Deleting, GeneratingTriples, TransformingTriples, _}
import io.renku.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.{Duration, Instant}

private class LongProcessingEventFinder[F[_]: Async: SessionResource](
    queriesExecTimes: LabeledHistogram[F],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[F, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  override val processName: ZombieEventProcess = ZombieEventProcess("lpe")

  override def popEvent(): F[Option[ZombieEvent]] = SessionResource[F].useK {
    queryProjectsToCheck >>= lookForZombie >>= markEventTaken
  }

  private type Candidate = (projects.Id, ProcessingStatus)

  private def queryProjectsToCheck = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - lpe - find projects")
      .select[Void, (projects.Id, EventStatus)](
        sql"""SELECT DISTINCT evt.project_id, evt.status
              FROM event evt
              WHERE evt.status IN (#${ProcessingStatus.all.map(s => show"'$s'").mkString(", ")})
          """
          .query(projectIdDecoder ~ eventStatusDecoder)
          .map { case id ~ status => (id, status) }
      )
      .arguments(Void)
      .build(_.toList)
      .flatMapResult(_.map {
        case (id, status: ProcessingStatus) => (id -> status).pure[F]
        case (_, status: EventStatus) =>
          new Exception(show"Long Processing Event finder cannot work with $status")
            .raiseError[F, (projects.Id, ProcessingStatus)]
      }.sequence)
  }

  private lazy val lookForZombie: List[Candidate] => Kleisli[F, Session[F], Option[ZombieEvent]] = {
    case Nil => Kleisli.pure(Option.empty[ZombieEvent])
    case (projectId, status) :: rest =>
      queryZombieEvent(projectId, status) >>= {
        case None              => lookForZombie(rest)
        case Some(zombieEvent) => Kleisli.pure(Option[ZombieEvent](zombieEvent))
      }
  }

  private def queryZombieEvent(projectId: projects.Id, status: ProcessingStatus) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - lpe - find")
      .select[
        projects.Id ~ EventStatus ~ String ~ ExecutionDate ~ EventProcessingTime ~ ExecutionDate ~ EventProcessingTime,
        ZombieEvent
      ](
        sql"""SELECT evt.event_id, evt.project_id, proj.project_path, evt.status
              FROM event evt
              JOIN project proj ON proj.project_id = evt.project_id
              LEFT JOIN event_delivery ed ON ed.project_id = evt.project_id AND ed.event_id = evt.event_id
              WHERE evt.project_id = $projectIdEncoder
                AND evt.status = $eventStatusEncoder
                AND (evt.message IS NULL OR evt.message <> $text)
                AND (
                  (ed.delivery_id IS NULL AND ($executionDateEncoder - evt.execution_date) > $eventProcessingTimeEncoder)
                  OR (ed.delivery_id IS NOT NULL AND ($executionDateEncoder - evt.execution_date) > $eventProcessingTimeEncoder)
                )
              LIMIT 1
              """
          .query(compoundEventIdDecoder ~ projectPathDecoder ~ processingStatusDecoder)
          .map { case id ~ path ~ status => ZombieEvent(processName, id, path, status) }
      )
      .arguments(
        projectId ~ status ~ zombieMessage ~
          ExecutionDate(now()) ~ EventProcessingTime(Duration ofMinutes 5) ~
          ExecutionDate(now()) ~ findGracePeriod(status)
      )
      .build(_.option)
  }

  private lazy val findGracePeriod: ProcessingStatus => EventProcessingTime = {
    case GeneratingTriples   => EventProcessingTime(Duration ofDays 4 * 7)
    case TransformingTriples => EventProcessingTime(Duration ofDays 1)
    case Deleting            => EventProcessingTime(Duration ofDays 1)
  }

  private lazy val markEventTaken: Option[ZombieEvent] => Kleisli[F, Session[F], Option[ZombieEvent]] = {
    case None        => Kleisli.pure(Option.empty[ZombieEvent])
    case Some(event) => updateMessage(event.eventId) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateMessage(eventId: CompoundEventId) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - lpe - update message")
      .command[String ~ ExecutionDate ~ EventId ~ projects.Id](
        sql"""UPDATE event
              SET message = $text, execution_date = $executionDateEncoder
              WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
            """.command
      )
      .arguments(zombieMessage ~ ExecutionDate(now()) ~ eventId.id ~ eventId.projectId)
      .build
  }

  private def toNoneIfEventAlreadyTaken(event: ZombieEvent): Completion => Option[ZombieEvent] = {
    case Completion.Update(1) => Some(event)
    case _                    => None
  }
}

private object LongProcessingEventFinder {
  def apply[F[_]: Async: SessionResource](queriesExecTimes: LabeledHistogram[F]): F[EventFinder[F, ZombieEvent]] =
    MonadThrow[F].catchNonFatal {
      new LongProcessingEventFinder(queriesExecTimes)
    }
}
