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

package io.renku.eventlog.subscriptions.awaitinggeneration

import cats.data.NonEmptyList
import cats.effect.{Bracket, ContextShift, IO}
import cats.free.Free
import cats.syntax.all._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import doobie.free.connection.ConnectionOp
import doobie.implicits._
import doobie.util.fragments._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog._
import io.renku.eventlog.subscriptions.awaitinggeneration.ProjectPrioritisation.{Priority, ProjectInfo}
import io.renku.eventlog.subscriptions.{EventFinder, ProjectIds, SubscriptionTypeSerializers}

import java.time.{Duration, Instant}
import scala.language.postfixOps
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

private class AwaitingGenerationEventFinderImpl(
    transactor:            DbTransactor[IO, EventLogDB],
    waitingEventsGauge:    LabeledGauge[IO, projects.Path],
    underProcessingGauge:  LabeledGauge[IO, projects.Path],
    queriesExecTimes:      LabeledHistogram[IO, SqlQuery.Name],
    now:                   () => Instant = () => Instant.now,
    maxProcessingTime:     Duration,
    projectsFetchingLimit: Int Refined Positive,
    projectPrioritisation: ProjectPrioritisation,
    pickRandomlyFrom:      List[ProjectIds] => Option[ProjectIds] = ids => ids.get(Random nextInt ids.size)
)(implicit ME:             Bracket[IO, Throwable], contextShift: ContextShift[IO])
    extends DbClient(Some(queriesExecTimes))
    with EventFinder[IO, AwaitingGenerationEvent]
    with SubscriptionTypeSerializers {

  override def popEvent(): IO[Option[AwaitingGenerationEvent]] =
    for {
      maybeProjectAwaitingGenerationEvent <- findEventAndUpdateForProcessing() transact transactor.get
      (maybeProject, maybeAwaitingGenerationEvent) = maybeProjectAwaitingGenerationEvent
      _ <- maybeUpdateMetrics(maybeProject, maybeAwaitingGenerationEvent)
    } yield maybeAwaitingGenerationEvent

  private def findEventAndUpdateForProcessing() =
    for {
      maybeProject <- measureExecutionTime(findProjectsWithEventsInQueue)
                        .map(projectPrioritisation.prioritise)
                        .map(selectProject)
      maybeIdAndProjectAndBody <- maybeProject
                                    .map(idAndPath => measureExecutionTime(findOldestEvent(idAndPath)))
                                    .getOrElse(Free.pure[ConnectionOp, Option[AwaitingGenerationEvent]](None))
      maybeBody <- markAsProcessing(maybeIdAndProjectAndBody)
    } yield maybeProject -> maybeBody

  // format: off
  private def findProjectsWithEventsInQueue = SqlQuery({fr"""
      SELECT
        proj.project_id,
        proj.project_path,
        proj.latest_event_date,
        (SELECT count(event_id) from event evt_int where evt_int.project_id = proj.project_id and evt_int.status = ${GeneratingTriples: EventStatus}) as current_occupancy
      FROM (
        SELECT DISTINCT
          proj.project_id,
          proj.project_path,
          proj.latest_event_date
        FROM event evt
        JOIN project proj on evt.project_id = proj.project_id
        WHERE ((""" ++ `status IN`(New, GenerationRecoverableFailure) ++ fr""" AND execution_date < ${now()})
          OR (status = ${GeneratingTriples: EventStatus} AND execution_date < ${now() minus maxProcessingTime})
        )
        ORDER BY proj.latest_event_date DESC
        LIMIT ${projectsFetchingLimit.value}
      ) proj
      """ 
    }.query[(projects.Id, projects.Path, EventDate, Int)]
    .map { case (projectId, projectPath, eventDate, currentOccupancy) => ProjectInfo(projectId, projectPath, eventDate, Refined.unsafeApply(currentOccupancy)) }
    .to[List],
    name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find projects")
  )
  // format: on

  // format: off
  private def findOldestEvent(idAndPath: ProjectIds) = SqlQuery({
    fr"""SELECT evt.event_id, evt.project_id, ${idAndPath.path} AS project_path, evt.event_body
         FROM (
           SELECT project_id, min(event_date) AS min_event_date
           FROM event
           WHERE project_id = ${idAndPath.id}
             AND ((""" ++ `status IN`(New, GenerationRecoverableFailure) ++ fr""" AND execution_date < ${now()})
               OR (status = ${GeneratingTriples: EventStatus} AND execution_date < ${now() minus maxProcessingTime}))
           GROUP BY project_id
         ) oldest_event_date
         JOIN event evt ON oldest_event_date.project_id = evt.project_id 
           AND oldest_event_date.min_event_date = evt.event_date
           AND ((""" ++ `status IN`(New, GenerationRecoverableFailure) ++ fr""" AND execution_date < ${now()})
               OR (status = ${GeneratingTriples: EventStatus} AND execution_date < ${now() minus maxProcessingTime})) 
         LIMIT 1
         """
    }.query[AwaitingGenerationEvent].option, 
    name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find oldest")
  )
  // format: on

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    in(fr"status", NonEmptyList.of(status, otherStatuses: _*))

  private lazy val selectProject: List[(ProjectIds, Priority)] => Option[ProjectIds] = {
    case Nil                          => None
    case (projectIdAndPath, _) +: Nil => Some(projectIdAndPath)
    case many                         => pickRandomlyFrom(prioritiesList(from = many))
  }

  private def prioritiesList(from: List[(ProjectIds, Priority)]): List[ProjectIds] =
    from.foldLeft(List.empty[ProjectIds]) { case (acc, (projectIdAndPath, priority)) =>
      acc :++ List.fill((priority.value * 10).setScale(2, RoundingMode.HALF_UP).toInt)(projectIdAndPath)
    }

  private lazy val markAsProcessing
      : Option[AwaitingGenerationEvent] => Free[ConnectionOp, Option[AwaitingGenerationEvent]] = {
    case None =>
      Free.pure[ConnectionOp, Option[AwaitingGenerationEvent]](None)
    case Some(event @ AwaitingGenerationEvent(id, _, _)) =>
      measureExecutionTime(updateStatus(id)) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateStatus(commitEventId: CompoundEventId) = SqlQuery(
    sql"""|UPDATE event 
          |SET status = ${GeneratingTriples: EventStatus}, execution_date = ${now()}
          |WHERE (event_id = ${commitEventId.id} AND project_id = ${commitEventId.projectId} AND status <> ${GeneratingTriples: EventStatus})
          |  OR (event_id = ${commitEventId.id} AND project_id = ${commitEventId.projectId} AND status = ${GeneratingTriples: EventStatus} AND execution_date < ${now() minus maxProcessingTime})
          |""".stripMargin.update.run,
    name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - update status")
  )

  private def toNoneIfEventAlreadyTaken(event: AwaitingGenerationEvent): Int => Option[AwaitingGenerationEvent] = {
    case 0 => None
    case 1 => Some(event)
  }

  private def maybeUpdateMetrics(maybeProject: Option[ProjectIds], maybeBody: Option[AwaitingGenerationEvent]) =
    (maybeBody, maybeProject) mapN { case (_, ProjectIds(_, projectPath)) =>
      for {
        _ <- waitingEventsGauge decrement projectPath
        _ <- underProcessingGauge increment projectPath
      } yield ()
    } getOrElse ME.unit
}

private object IOAwaitingGenerationEventFinder {

  private val MaxProcessingTime:     Duration             = Duration.ofHours(24)
  private val ProjectsFetchingLimit: Int Refined Positive = 10

  def apply(
      transactor:           DbTransactor[IO, EventLogDB],
      waitingEventsGauge:   LabeledGauge[IO, projects.Path],
      underProcessingGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:     LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift:  ContextShift[IO]): IO[EventFinder[IO, AwaitingGenerationEvent]] = IO {
    new AwaitingGenerationEventFinderImpl(transactor,
                                          waitingEventsGauge,
                                          underProcessingGauge,
                                          queriesExecTimes,
                                          maxProcessingTime = MaxProcessingTime,
                                          projectsFetchingLimit = ProjectsFetchingLimit,
                                          projectPrioritisation = new ProjectPrioritisation()
    )
  }
}
