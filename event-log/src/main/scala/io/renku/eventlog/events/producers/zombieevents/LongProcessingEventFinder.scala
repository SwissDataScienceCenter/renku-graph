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

package io.renku.eventlog.events.producers
package zombieevents

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import io.renku.db.implicits._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.events.producers
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus, ExecutionDate}
import io.renku.graph.model.projects
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.{Duration, Instant}

private class LongProcessingEventFinder[F[_]: Async: SessionResource: QueriesExecutionTimes](
    now: () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with producers.EventFinder[F, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  override val processName: ZombieEventProcess = ZombieEventProcess("lpe")

  override def popEvent(): F[Option[ZombieEvent]] = SessionResource[F].useK {
    queryProjectsToCheck >>= lookForZombie >>= markEventTaken
  }

  private type Candidate = (projects.GitLabId, ProcessingStatus)

  private def queryProjectsToCheck = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - lpe - find projects")
      .select[Void, (projects.GitLabId, EventStatus)](
        sql"""SELECT DISTINCT evt.project_id, evt.status
              FROM event evt
              WHERE evt.status IN (#${ProcessingStatus.all.map(s => show"'$s'").mkString(", ")})
          """
          .query(projectIdDecoder ~ eventStatusDecoder)
      )
      .arguments(Void)
      .build(_.toList)
      .flatMapResult(_.map {
        case (id, status: ProcessingStatus) => (id -> status).pure[F]
        case (_, status: EventStatus) =>
          new Exception(show"Long Processing Event finder cannot work with $status")
            .raiseError[F, (projects.GitLabId, ProcessingStatus)]
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

  private def queryZombieEvent(projectId: projects.GitLabId, status: ProcessingStatus) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - lpe - find")
      .select[
        projects.GitLabId *: EventStatus *: String *: ExecutionDate *: EventProcessingTime *: ExecutionDate *: EventProcessingTime *: EmptyTuple,
        ZombieEvent
      ](
        sql"""SELECT evt.event_id, evt.project_id, proj.project_slug, evt.status
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
          .query(compoundEventIdDecoder ~ projectSlugDecoder ~ processingStatusDecoder)
          .map { case id ~ slug ~ status => ZombieEvent(processName, id, slug, status) }
      )
      .arguments(
        projectId *: status *: zombieMessage *:
          ExecutionDate(now()) *: EventProcessingTime(Duration ofMinutes 10) *:
          ExecutionDate(now()) *: findGracePeriod(status) *: EmptyTuple
      )
      .build(_.option)
  }

  private lazy val findGracePeriod: ProcessingStatus => EventProcessingTime = {
    case GeneratingTriples   => EventProcessingTime(Duration ofDays 6 * 7)
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
      .command[String *: ExecutionDate *: EventId *: projects.GitLabId *: EmptyTuple](
        sql"""UPDATE event
              SET message = $text, execution_date = $executionDateEncoder
              WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
            """.command
      )
      .arguments(zombieMessage *: ExecutionDate(now()) *: eventId.id *: eventId.projectId *: EmptyTuple)
      .build
  }

  private def toNoneIfEventAlreadyTaken(event: ZombieEvent): Completion => Option[ZombieEvent] = {
    case Completion.Update(1) => Some(event)
    case _                    => None
  }
}

private object LongProcessingEventFinder {
  def apply[F[_]: Async: SessionResource: QueriesExecutionTimes]: F[producers.EventFinder[F, ZombieEvent]] =
    MonadThrow[F].catchNonFatal(new LongProcessingEventFinder())
}
