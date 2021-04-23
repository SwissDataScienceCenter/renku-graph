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
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.db.implicits._
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
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[Interpretation, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  override def popEvent(): Interpretation[Option[ZombieEvent]] = sessionResource.useK {
    findPotentialZombies >>= lookForZombie >>= markEventTaken
  }

  private lazy val findPotentialZombies
      : Kleisli[Interpretation, Session[Interpretation], List[(projects.Id, EventStatus)]] =
    Nested(queryProjectsToCheck).map { case (id, currentStatus) => id -> currentStatus.toEventStatus }.value

  private def queryProjectsToCheck = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lpe - find projects"))
      .select[EventStatus ~ EventStatus, (projects.Id, TransformationStatus)](
        sql"""SELECT DISTINCT evt.project_id, evt.status
                FROM event evt
                WHERE evt.status = $eventStatusEncoder
                  OR evt.status = $eventStatusEncoder
          """
          .query(projectIdDecoder ~ transformationStatusGet)
          .map { case id ~ status => (id, status) }
      )
      .arguments(GeneratingTriples ~ TransformingTriples)
      .build(_.toList)

  }

  private lazy val lookForZombie
      : List[(projects.Id, EventStatus)] => Kleisli[Interpretation, Session[Interpretation], Option[ZombieEvent]] = {
    case Nil => Kleisli.pure(Option.empty[ZombieEvent])
    case (projectId, status) :: rest =>
      queryZombieEvent(projectId, status) flatMap {
        case None              => lookForZombie(rest)
        case Some(zombieEvent) => Kleisli.pure(Option[ZombieEvent](zombieEvent))
      }
  }

  private def queryZombieEvent(projectId: projects.Id, status: EventStatus) =
    measureExecutionTime {
      SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lpe - find"))
        .select[projects.Id ~ EventStatus ~ String ~ ExecutionDate ~ EventProcessingTime, ZombieEvent](
          sql"""SELECT evt.event_id, evt.project_id, proj.project_path, evt.status
                  FROM event evt
                  JOIN project proj ON proj.project_id = evt.project_id
                  LEFT JOIN event_delivery ed ON ed.project_id = evt.project_id AND ed.event_id = evt.event_id
                  WHERE evt.project_id = $projectIdEncoder
                    AND evt.status = $eventStatusEncoder
                    AND (evt.message IS NULL OR evt.message <> $text)
                    AND (ed.delivery_id IS NULL
                      AND ($executionDateEncoder - evt.execution_date) > $eventProcessingTimeEncoder
                    )
                  LIMIT 1
                  """
            .query(compoundEventIdDecoder ~ projectPathDecoder ~ eventStatusDecoder)
            .map { case id ~ path ~ status => ZombieEvent(processName, id, path, status) }
        )
        .arguments(
          projectId ~ status ~ zombieMessage ~ ExecutionDate(now()) ~ EventProcessingTime(Duration.ofMinutes(5))
        )
        .build(_.option)
    }

  private lazy val markEventTaken
      : Option[ZombieEvent] => Kleisli[Interpretation, Session[Interpretation], Option[ZombieEvent]] = {
    case None        => Kleisli.pure(Option.empty[ZombieEvent])
    case Some(event) => updateMessage(event.eventId) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateMessage(eventId: CompoundEventId) =
    measureExecutionTime {
      SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lpe - update message"))
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

  override val processName: ZombieEventProcess = ZombieEventProcess("lpe")

  private implicit val transformationStatusGet: Decoder[TransformationStatus] = varchar.map {
    case GeneratingTriples.value   => TransformationStatus.Generating
    case TransformingTriples.value => TransformationStatus.Transforming
    case other                     => throw new Exception(s"${getClass.getName} cannot work with $other")
  }

  private sealed trait TransformationStatus {
    val toEventStatus: EventStatus

    def processingTimeFindingStatus: EventStatus

    def followingFindingStatuses: Set[EventStatus]
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
      sessionResource:     SessionResource[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlStatement.Name]
  )(implicit contextShift: ContextShift[IO]): IO[EventFinder[IO, ZombieEvent]] = IO {
    new LongProcessingEventFinder(sessionResource, queriesExecTimes)
  }
}
