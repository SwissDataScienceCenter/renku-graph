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

package io.renku.eventlog.subscriptions.awaitinggeneration

import cats.data.Kleisli
import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import cats.{Id, Parallel}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.db.implicits._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog._
import io.renku.eventlog.subscriptions.awaitinggeneration.ProjectPrioritisation.{Priority, ProjectInfo}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram}
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

private class AwaitingGenerationEventFinderImpl[F[_]: MonadCancelThrow: Async: Parallel: SessionResource](
    waitingEventsGauge:    LabeledGauge[F, projects.Path],
    underProcessingGauge:  LabeledGauge[F, projects.Path],
    queriesExecTimes:      LabeledHistogram[F],
    now:                   () => Instant = () => Instant.now,
    projectsFetchingLimit: Int Refined Positive,
    projectPrioritisation: ProjectPrioritisation[F],
    pickRandomlyFrom: List[subscriptions.ProjectIds] => Option[subscriptions.ProjectIds] = ids =>
      ids.get(Random nextInt ids.size)
) extends DbClient(Some(queriesExecTimes))
    with subscriptions.EventFinder[F, AwaitingGenerationEvent]
    with subscriptions.SubscriptionTypeSerializers
    with TypeSerializers {

  import projectPrioritisation._

  override def popEvent(): F[Option[AwaitingGenerationEvent]] = SessionResource[F].useK {
    for {
      maybeProjectAwaitingGenerationEvent <- findEventAndUpdateForProcessing
      (maybeProject, maybeAwaitingGenerationEvent) = maybeProjectAwaitingGenerationEvent
      _ <- maybeUpdateMetrics(maybeProject, maybeAwaitingGenerationEvent)
    } yield maybeAwaitingGenerationEvent
  }

  private def findEventAndUpdateForProcessing = for {
    maybeProject <- selectCandidateProject()
    maybeIdAndProjectAndBody <- maybeProject
                                  .map(findLatestEvent)
                                  .getOrElse(Kleisli.pure(Option.empty[AwaitingGenerationEvent]))
    maybeBody <- markAsProcessing(maybeIdAndProjectAndBody)
  } yield maybeProject -> maybeBody

  private def selectCandidateProject() =
    (findProjectsWithEventsInQueue, findTotalOccupancy) parMapN { case (potentialProjects, totalOccupancy) =>
      ((prioritise _).tupled >>> selectProject)(potentialProjects, totalOccupancy)
    }

  private def findProjectsWithEventsInQueue = measureExecutionTime {
    SqlStatement(
      name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find projects")
    ).select[ExecutionDate ~ ExecutionDate ~ Int, ProjectInfo](
      sql"""
      SELECT p.project_id, p.project_path, p.latest_event_date,
	    (SELECT count(event_id) FROM event evt_int WHERE evt_int.project_id = p.project_id AND evt_int.status = '#${GeneratingTriples.value}') AS current_occupancy
      FROM (
        SELECT DISTINCT project_id
        FROM event
        WHERE status IN ('#${New.value}', '#${GenerationRecoverableFailure.value}')
          AND execution_date <= $executionDateEncoder
      ) candidate_projects
      JOIN project p ON p.project_id = candidate_projects.project_id
      JOIN event e ON e.project_id = candidate_projects.project_id 
        AND e.event_date = p.latest_event_date
        AND e.execution_date <= $executionDateEncoder
        AND e.status IN ('#${New.value}', '#${GenerationRecoverableFailure.value}', '#${GenerationNonRecoverableFailure.value}', '#${TransformationNonRecoverableFailure.value}', '#${Skipped.value}')
      ORDER BY p.latest_event_date DESC
      LIMIT $int4
      """
        .query(projectIdDecoder ~ projectPathDecoder ~ eventDateDecoder ~ int8)
        .map { case (id: projects.Id) ~ (path: projects.Path) ~ (eventDate: EventDate) ~ (currentOccupancy: Long) =>
          ProjectInfo(id, path, eventDate, Refined.unsafeApply(currentOccupancy.toInt))
        }
    ).arguments(ExecutionDate(now()) ~ ExecutionDate(now()) ~ projectsFetchingLimit.value)
      .build(_.toList)
  }

  private def findTotalOccupancy = measureExecutionTime {
    SqlStatement(
      name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find total occupancy")
    ).select[EventStatus, Long](
      sql"""SELECT count(event_id) FROM event WHERE status = $eventStatusEncoder""".query(int8)
    ).arguments(GeneratingTriples)
      .build[Id](_.unique)
  }

  private def findLatestEvent(idAndPath: subscriptions.ProjectIds) = measureExecutionTime {
    val executionDate = ExecutionDate(now())
    SqlStatement(
      name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find latest")
    ).select[projects.Path ~ projects.Id ~ ExecutionDate ~ ExecutionDate, AwaitingGenerationEvent](
      sql"""
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
       """
        .query(awaitingGenerationEventGet)
    ).arguments(idAndPath.path ~ idAndPath.id ~ executionDate ~ executionDate)
      .build(_.option)
  }

  private lazy val selectProject: List[(subscriptions.ProjectIds, Priority)] => Option[subscriptions.ProjectIds] = {
    case Nil                          => None
    case (projectIdAndPath, _) :: Nil => Some(projectIdAndPath)
    case many                         => pickRandomlyFrom(prioritiesList(from = many))
  }

  private def prioritiesList(from: List[(subscriptions.ProjectIds, Priority)]): List[subscriptions.ProjectIds] =
    from.foldLeft(List.empty[subscriptions.ProjectIds]) { case (acc, (projectIdAndPath, priority)) =>
      acc :++ List.fill((priority.value * 10).setScale(2, RoundingMode.HALF_UP).toInt)(projectIdAndPath)
    }

  private lazy val markAsProcessing
      : Option[AwaitingGenerationEvent] => Kleisli[F, Session[F], Option[AwaitingGenerationEvent]] = {
    case None =>
      Kleisli.pure(Option.empty[AwaitingGenerationEvent])
    case Some(event @ AwaitingGenerationEvent(id, _, _)) =>
      measureExecutionTime(updateStatus(id)) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateStatus(commitEventId: CompoundEventId) =
    SqlStatement[F](name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - update status"))
      .command[EventStatus ~ ExecutionDate ~ EventId ~ projects.Id ~ EventStatus](
        sql"""
        UPDATE event 
        SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
        WHERE event_id = $eventIdEncoder
          AND project_id = $projectIdEncoder
          AND status <> $eventStatusEncoder
        """.command
      )
      .arguments(
        GeneratingTriples ~ ExecutionDate(now()) ~ commitEventId.id ~ commitEventId.projectId ~ GeneratingTriples
      )
      .build

  private def toNoneIfEventAlreadyTaken(
      event: AwaitingGenerationEvent
  ): Completion => Option[AwaitingGenerationEvent] = {
    case Completion.Update(1) => Some(event)
    case _                    => None
  }

  private def maybeUpdateMetrics(maybeProject: Option[subscriptions.ProjectIds],
                                 maybeBody:    Option[AwaitingGenerationEvent]
  ) = (maybeBody, maybeProject) mapN { case (_, subscriptions.ProjectIds(_, projectPath)) =>
    Kleisli.liftF {
      for {
        _ <- waitingEventsGauge decrement projectPath
        _ <- underProcessingGauge increment projectPath
      } yield ()
    }
  } getOrElse Kleisli.pure[F, Session[F], Unit](())

  private val awaitingGenerationEventGet: Decoder[AwaitingGenerationEvent] =
    (compoundEventIdDecoder ~ projectPathDecoder ~ eventBodyDecoder).gmap[AwaitingGenerationEvent]
}

private object AwaitingGenerationEventFinder {

  private val ProjectsFetchingLimit: Int Refined Positive = 20

  def apply[F[_]: MonadCancelThrow: Async: Parallel: SessionResource](
      subscribers:          subscriptions.Subscribers[F],
      waitingEventsGauge:   LabeledGauge[F, projects.Path],
      underProcessingGauge: LabeledGauge[F, projects.Path],
      queriesExecTimes:     LabeledHistogram[F]
  ): F[subscriptions.EventFinder[F, AwaitingGenerationEvent]] = for {
    projectPrioritisation <- ProjectPrioritisation(subscribers)
  } yield new AwaitingGenerationEventFinderImpl(waitingEventsGauge,
                                                underProcessingGauge,
                                                queriesExecTimes,
                                                projectsFetchingLimit = ProjectsFetchingLimit,
                                                projectPrioritisation = projectPrioritisation
  )
}
