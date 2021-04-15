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

package io.renku.eventlog.subscriptions.zombieevents

import cats.data.{Kleisli, Nested}
import cats.effect.{Async, Bracket, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import io.renku.eventlog.subscriptions.EventFinder
import io.renku.eventlog.{EventLogDB, ExecutionDate, TypeSerializers}
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.{Duration, Instant}

private class LongProcessingEventFinder[Interpretation[_]: Async: Bracket[*[_], Throwable]: ContextShift](
    transactor:       SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[Interpretation, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  override def popEvent(): Interpretation[Option[ZombieEvent]] = transactor.use { implicit session =>
    session.transaction.use { transaction =>
      for {
        sp <- transaction.savepoint
        result <- (findPotentialZombies >>= lookForZombie >>= markEventTaken) recoverWith { error =>
                    transaction.rollback(sp).flatMap(_ => error.raiseError[Interpretation, Option[ZombieEvent]])
                  }
      } yield result
    }
  }

  private def findPotentialZombies(implicit
      session: Session[Interpretation]
  ): Interpretation[List[(projects.Id, EventStatus)]] =
    Nested(queryProjectsToCheck).map { case (id, currentStatus) => id -> currentStatus.toEventStatus }.value

  private def queryProjectsToCheck(implicit session: Session[Interpretation]) = measureExecutionTime {
    SqlQuery[Interpretation, List[(projects.Id, TransformationStatus)]](
      Kleisli { session =>
        val query: Query[EventStatus ~ EventStatus, (projects.Id, TransformationStatus)] = sql"""
          SELECT DISTINCT evt.project_id, evt.status
          FROM event evt
          WHERE evt.status = $eventStatusPut
            OR evt.status = $eventStatusPut
          """
          .query(projectIdGet ~ transformationStatusGet)
          .map { case id ~ status => (id, status) }

        session.prepare(query).use(_.stream(GeneratingTriples ~ TransformingTriples, 32).compile.toList)
      },
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lpe - find projects")
    )
  }

  private def lookForZombie(implicit
      session: Session[Interpretation]
  ): List[(projects.Id, EventStatus)] => Interpretation[Option[ZombieEvent]] = {
    case Nil => Option.empty[ZombieEvent].pure[Interpretation]
    case (projectId, status) :: rest =>
      queryZombieEvent(projectId, status) flatMap {
        case None              => lookForZombie(session)(rest)
        case Some(zombieEvent) => Option[ZombieEvent](zombieEvent).pure[Interpretation]
      }
  }

  private def queryZombieEvent(projectId: projects.Id, status: EventStatus)(implicit session: Session[Interpretation]) =
    measureExecutionTime {
      SqlQuery[Interpretation, Option[ZombieEvent]](
        Kleisli { session =>
          val query: Query[projects.Id ~ EventStatus ~ String ~ ExecutionDate ~ EventProcessingTime, ZombieEvent] =
            sql"""SELECT evt.event_id, evt.project_id, proj.project_path, evt.status
                  FROM event evt
                  JOIN project proj ON proj.project_id = evt.project_id
                  LEFT JOIN event_delivery ed ON ed.project_id = evt.project_id AND ed.event_id = evt.event_id
                  WHERE evt.project_id = $projectIdPut
                    AND evt.status = $eventStatusPut
                    AND (evt.message IS NULL OR evt.message <> $text)
                    AND (ed.delivery_id IS NULL
                      AND ($executionDatePut - evt.execution_date) > $eventProcessingTimePut
                    )
                  LIMIT 1
                  """
              .query(compoundEventIdGet ~ projectPathGet ~ eventStatusGet)
              .map { case id ~ path ~ status => ZombieEvent(processName, id, path, status) }

          session
            .prepare(query)
            .use(
              _.option(
                projectId ~ status ~ zombieMessage ~ ExecutionDate(now()) ~ EventProcessingTime(Duration.ofMinutes(5))
              )
            )
        },
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lpe - find")
      )
    }

  private def markEventTaken(implicit
      session: Session[Interpretation]
  ): Option[ZombieEvent] => Interpretation[Option[ZombieEvent]] = {
    case None        => Option.empty[ZombieEvent].pure[Interpretation]
    case Some(event) => updateMessage(event.eventId) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateMessage(eventId: CompoundEventId)(implicit session: Session[Interpretation]) =
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Command[String ~ ExecutionDate ~ EventId ~ projects.Id] = sql"""
          UPDATE event
          SET message = $text, execution_date = $executionDatePut
          WHERE event_id = $eventIdPut AND project_id = $projectIdPut
          """.command
          session.prepare(query).use(_.execute(zombieMessage ~ ExecutionDate(now()) ~ eventId.id ~ eventId.projectId))
        },
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lpe - update message")
      )
    }

  private def toNoneIfEventAlreadyTaken(event: ZombieEvent): Completion => Option[ZombieEvent] = {
    case Completion.Update(1) => Some(event)
    case _                    => None
  }

  override val processName: ZombieEventProcess = ZombieEventProcess("lpe")

  private implicit val transformationStatusGet: Decoder[TransformationStatus] = varchar.map {
    case GeneratingTriples.value   => TransformationStatus.Generating
    case TransformingTriples.value => TransformationStatus.Transforming
    case other                     => throw new Exception(s"${getClass.getName} cannot work with $other")
  }
  private sealed trait TransformationStatus {
    val toEventStatus: EventStatus
    def processingTimeFindingStatus: EventStatus
    def followingFindingStatuses:    Set[EventStatus]
  }
  private object TransformationStatus {
    final case object Generating extends TransformationStatus {
      override val toEventStatus:               EventStatus = GeneratingTriples
      override val processingTimeFindingStatus: EventStatus = TriplesGenerated
      override val followingFindingStatuses: Set[EventStatus] = Set(TriplesGenerated,
                                                                    TransformingTriples,
                                                                    TransformationRecoverableFailure,
                                                                    TransformationNonRecoverableFailure,
                                                                    TriplesStore
      )
    }
    final case object Transforming extends TransformationStatus {
      override val toEventStatus:               EventStatus      = TransformingTriples
      override val processingTimeFindingStatus: EventStatus      = TriplesStore
      override val followingFindingStatuses:    Set[EventStatus] = Set(TriplesStore)
    }
  }
}

private object LongProcessingEventFinder {

  def apply(
      transactor:          SessionResource[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[EventFinder[IO, ZombieEvent]] = IO {
    new LongProcessingEventFinder(transactor, queriesExecTimes)
  }
}
