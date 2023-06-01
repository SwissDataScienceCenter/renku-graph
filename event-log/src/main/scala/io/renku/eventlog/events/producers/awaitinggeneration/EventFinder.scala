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

package io.renku.eventlog.events.producers.awaitinggeneration

import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import cats.{Id, Parallel}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.db.implicits._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.events.producers
import io.renku.eventlog.events.producers.DefaultSubscribers.DefaultSubscribers
import io.renku.eventlog.events.producers.ProjectPrioritisation.{Priority, ProjectInfo}
import io.renku.eventlog.events.producers.{EventFinder, ProjectPrioritisation}
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventDate, EventId, EventStatus, ExecutionDate}
import io.renku.graph.model.projects
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

private class EventFinderImpl[F[_]: Async: Parallel: SessionResource: QueriesExecutionTimes: EventStatusGauges](
    now:                   () => Instant = () => Instant.now,
    projectsFetchingLimit: Int Refined Positive,
    projectPrioritisation: ProjectPrioritisation[F],
    pickRandomly: List[producers.ProjectIds] => Option[producers.ProjectIds] = ids => ids.get(Random nextInt ids.size)
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with producers.EventFinder[F, AwaitingGenerationEvent]
    with producers.SubscriptionTypeSerializers
    with TypeSerializers {

  import projectPrioritisation._

  override def popEvent(): F[Option[AwaitingGenerationEvent]] = SessionResource[F].useK {
    for {
      (maybeProject, maybeAwaitingGenerationEvent) <- findEventAndUpdateForProcessing
      _                                            <- maybeUpdateMetrics(maybeProject, maybeAwaitingGenerationEvent)
    } yield maybeAwaitingGenerationEvent
  }

  private def findEventAndUpdateForProcessing = for {
    maybeProject <- selectCandidateProject
    maybeIdAndProjectAndBody <- maybeProject
                                  .map(findLatestEvent)
                                  .getOrElse(Kleisli.pure(Option.empty[AwaitingGenerationEvent]))
    maybeBody <- markAsProcessing(maybeIdAndProjectAndBody)
  } yield maybeProject -> maybeBody

  private def selectCandidateProject = for {
    projects       <- findProjectsWithEventsInQueue
    totalOccupancy <- findTotalOccupancy
  } yield ((prioritise _).tupled >>> selectProject)(projects, totalOccupancy)

  private def findProjectsWithEventsInQueue = measureExecutionTime {
    SqlStatement
      .named(s"${SubscriptionCategory.categoryName.value.toLowerCase} - find projects")
      .select[ExecutionDate *: ExecutionDate *: Int *: EmptyTuple, ProjectInfo](
        sql"""
          SELECT p.project_id, p.project_path, p.latest_event_date,
	        (SELECT count(event_id) FROM event evt_int WHERE evt_int.project_id = p.project_id AND evt_int.status = '#${GeneratingTriples.value}') AS current_occupancy
          FROM (
            SELECT DISTINCT project_id, MAX(event_date) AS max_event_date
            FROM event
            WHERE status IN ('#${New.value}', '#${GenerationRecoverableFailure.value}')
              AND execution_date <= $executionDateEncoder
            GROUP BY project_id
          ) candidate_projects
          JOIN project p ON p.project_id = candidate_projects.project_id
          JOIN LATERAL (
            SELECT COUNT(ee.event_id) AS count
	        FROM event ee
	        WHERE ee.project_id = candidate_projects.project_id
	          AND ee.event_date > candidate_projects.max_event_date
	          AND ee.status IN ('#${GeneratingTriples.value}', '#${TriplesGenerated.value}', '#${GenerationRecoverableFailure.value}', '#${TransformingTriples.value}', '#${TriplesStore.value}', '#${TransformationRecoverableFailure.value}', '#${AwaitingDeletion.value}', '#${Deleting.value}')
	      ) younger_statuses_final ON 1 = 1
          JOIN LATERAL (
            SELECT COUNT(ee.event_id) AS count
	        FROM event ee
	        WHERE ee.project_id = candidate_projects.project_id
              AND ee.execution_date <= $executionDateEncoder
	          AND ee.event_date = candidate_projects.max_event_date
	          AND ee.status IN ('#${New.value}', '#${GenerationRecoverableFailure.value}')
	      ) same_date_statuses_to_do ON 1 = 1
          WHERE younger_statuses_final.count = 0
            AND same_date_statuses_to_do.count > 0
          ORDER BY p.latest_event_date DESC
          LIMIT $int4
          """
          .query(projectIdDecoder ~ projectPathDecoder ~ eventDateDecoder ~ int8)
          .map {
            case (id: projects.GitLabId) ~ (path: projects.Path) ~ (eventDate: EventDate) ~ (currentOccupancy: Long) =>
              ProjectInfo(id, path, eventDate, Refined.unsafeApply(currentOccupancy.toInt))
          }
      )
      .arguments(ExecutionDate(now()) *: ExecutionDate(now()) *: projectsFetchingLimit.value *: EmptyTuple)
      .build(_.toList)
  }

  private def findTotalOccupancy = measureExecutionTime {
    SqlStatement
      .named(s"${SubscriptionCategory.categoryName.value.toLowerCase} - find total occupancy")
      .select[Void, Long](
        sql"""SELECT COUNT(event_id) FROM event WHERE status = '#${GeneratingTriples.value}'""".query(int8)
      )
      .arguments(Void)
      .build[Id](_.unique)
  }

  private def findLatestEvent(idAndPath: producers.ProjectIds) = measureExecutionTime {
    val executionDate = ExecutionDate(now())
    SqlStatement
      .named(s"${SubscriptionCategory.categoryName.value.toLowerCase} - find latest")
      .select[projects.Path *: projects.GitLabId *: ExecutionDate *: ExecutionDate *: EmptyTuple,
              AwaitingGenerationEvent
      ](sql"""
        SELECT evt.event_id, evt.project_id, $projectPathEncoder AS project_path, evt.event_body
        FROM (
          SELECT project_id, max(event_date) AS max_event_date
          FROM event
          WHERE project_id = $projectIdEncoder
            AND status IN ('#${New.value}', '#${GenerationRecoverableFailure.value}')
            AND execution_date < $executionDateEncoder
          GROUP BY project_id
        ) newest_event_date
        JOIN event evt ON newest_event_date.project_id = evt.project_id
          AND newest_event_date.max_event_date = evt.event_date
          AND status IN ('#${New.value}', '#${GenerationRecoverableFailure.value}')
          AND execution_date < $executionDateEncoder
        LIMIT 1
        """.query(awaitingGenerationEventGet))
      .arguments(idAndPath.path *: idAndPath.id *: executionDate *: executionDate *: EmptyTuple)
      .build(_.option)
  }

  private lazy val selectProject: List[(producers.ProjectIds, Priority)] => Option[producers.ProjectIds] = {
    case Nil                          => None
    case (projectIdAndPath, _) :: Nil => Some(projectIdAndPath)
    case many                         => pickRandomly(prioritiesList(from = many))
  }

  private def prioritiesList(from: List[(producers.ProjectIds, Priority)]): List[producers.ProjectIds] =
    from.foldLeft(List.empty[producers.ProjectIds]) { case (acc, (projectIdAndPath, priority)) =>
      acc :++ List.fill((priority.value * 10).setScale(2, RoundingMode.HALF_UP).toInt)(projectIdAndPath)
    }

  private lazy val markAsProcessing
      : Option[AwaitingGenerationEvent] => Kleisli[F, Session[F], Option[AwaitingGenerationEvent]] = {
    case None                                            => Kleisli.pure(Option.empty[AwaitingGenerationEvent])
    case Some(event @ AwaitingGenerationEvent(id, _, _)) => updateStatus(id) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateStatus(commitEventId: CompoundEventId) = measureExecutionTime {
    SqlStatement
      .named(s"${SubscriptionCategory.categoryName.value.toLowerCase} - update status")
      .command[EventStatus *: ExecutionDate *: EventId *: projects.GitLabId *: EventStatus *: EmptyTuple](sql"""
        UPDATE event 
        SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
        WHERE event_id = $eventIdEncoder
          AND project_id = $projectIdEncoder
          AND status <> $eventStatusEncoder
        """.command)
      .arguments(
        GeneratingTriples *:
          ExecutionDate(now()) *:
          commitEventId.id *:
          commitEventId.projectId *:
          GeneratingTriples *:
          EmptyTuple
      )
      .build
  }

  private def toNoneIfEventAlreadyTaken(
      event: AwaitingGenerationEvent
  ): Completion => Option[AwaitingGenerationEvent] = {
    case Completion.Update(1) => Some(event)
    case _                    => None
  }

  private def maybeUpdateMetrics(maybeProject: Option[producers.ProjectIds],
                                 maybeBody:    Option[AwaitingGenerationEvent]
  ) = (maybeBody, maybeProject) mapN { case (_, producers.ProjectIds(_, projectPath)) =>
    Kleisli.liftF {
      (EventStatusGauges[F].awaitingGeneration decrement projectPath) >>
        (EventStatusGauges[F].underGeneration increment projectPath)
    }
  } getOrElse Kleisli.pure[F, Session[F], Unit](())

  private val awaitingGenerationEventGet: Decoder[AwaitingGenerationEvent] =
    (compoundEventIdDecoder *: projectPathDecoder *: eventBodyDecoder).to[AwaitingGenerationEvent]
}

private object EventFinder {

  private val ProjectsFetchingLimit: Int Refined Positive = 20

  def apply[F[_]: Async: Parallel: DefaultSubscribers: SessionResource: QueriesExecutionTimes: EventStatusGauges]
      : F[EventFinder[F, AwaitingGenerationEvent]] =
    ProjectPrioritisation[F]
      .map(pp => new EventFinderImpl(projectsFetchingLimit = ProjectsFetchingLimit, projectPrioritisation = pp))
}
