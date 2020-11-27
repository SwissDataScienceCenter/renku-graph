/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions.unprocessed

import java.time.{Duration, Instant}

import cats.data.NonEmptyList
import cats.effect.{Bracket, ContextShift, IO}
import cats.free.Free
import cats.syntax.all._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventBody, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import doobie.free.connection.ConnectionOp
import doobie.implicits._
import doobie.util.fragments._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog._
import io.renku.eventlog.subscriptions.{EventFetcher, ProjectIds}
import io.renku.eventlog.subscriptions.unprocessed.ProjectPrioritisation.{Priority, ProjectInfo}

import scala.language.postfixOps
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

private[subscriptions] class UnprocessedEventFetcherImpl(
    transactor:            DbTransactor[IO, EventLogDB],
    waitingEventsGauge:    LabeledGauge[IO, projects.Path],
    underProcessingGauge:  LabeledGauge[IO, projects.Path],
    queriesExecTimes:      LabeledHistogram[IO, SqlQuery.Name],
    now:                   () => Instant = () => Instant.now,
    maxProcessingTime:     Duration,
    projectsFetchingLimit: Int Refined Positive,
    projectPrioritisation: ProjectPrioritisation,
    pickRandomlyFrom:      List[ProjectIds] => Option[ProjectIds] = ids => ids.get(Random nextInt ids.size),
    waitForViewRefresh:    Boolean = false
)(implicit ME:             Bracket[IO, Throwable], contextShift: ContextShift[IO])
    extends DbClient(Some(queriesExecTimes))
    with EventFetcher[IO]
    with TypesSerializers {

  private type EventIdAndBody = (CompoundEventId, EventBody)

  override def popEvent(): IO[Option[EventIdAndBody]] =
    for {
      maybeProjectEventIdAndBody <- findEventAndUpdateForProcessing() transact transactor.get
      (maybeProject, maybeEventIdAndBody) = maybeProjectEventIdAndBody
      _ <- maybeUpdateMetrics(maybeProject, maybeEventIdAndBody)
    } yield maybeEventIdAndBody

  private def findEventAndUpdateForProcessing() =
    for {
      maybeProject <- measureExecutionTime(findProjectsWithEventsInQueue)
                        .map(projectPrioritisation.prioritise)
                        .map(selectProject)
      maybeIdAndProjectAndBody <- maybeProject
                                    .map(idAndPath => measureExecutionTime(findOldestEvent(idAndPath)))
                                    .getOrElse(Free.pure[ConnectionOp, Option[EventIdAndBody]](None))
      maybeBody <- markAsProcessing(maybeIdAndProjectAndBody)
    } yield maybeProject -> maybeBody

  // format: off
  private def findProjectsWithEventsInQueue = SqlQuery({fr"""
      SELECT 
        proj.project_id,
        proj.project_path,
        proj.latest_event_date,
        (SELECT count(event_id) from event evt_int where evt_int.project_id = proj.project_id and evt_int.status = ${Processing: EventStatus}) as current_occupancy 
      FROM (SELECT project_id, project_path, latest_event_date 
            FROM project 
            ORDER BY latest_event_date desc) proj
      WHERE EXISTS (
        SELECT project_id
        FROM event evt
        WHERE evt.project_id = proj.project_id
          AND ((""" ++ `status IN`(New, RecoverableFailure) ++ fr""" AND execution_date < ${now()})
            OR (status = ${Processing: EventStatus} AND execution_date < ${now() minus maxProcessingTime})
          )
      )
      ORDER BY latest_event_date DESC
      LIMIT ${projectsFetchingLimit.value}  
      """ 
    }.query[(projects.Id, projects.Path, EventDate, Int)]
    .map { case (projectId, projectPath, eventDate, currentOccupancy) => ProjectInfo(projectId, projectPath, eventDate, Refined.unsafeApply(currentOccupancy)) }
    .to[List],
    name = "pop event - projects"
  )
  // format: on

  // format: off
  private def findOldestEvent(idAndPath: ProjectIds) = SqlQuery({
    fr"""SELECT evt.event_id, evt.project_id, evt.event_body
         FROM (
           SELECT project_id, min(event_date) as min_event_date
           FROM event
           WHERE project_id = ${idAndPath.id}
             AND ((""" ++ `status IN`(New, RecoverableFailure) ++ fr""" AND execution_date < ${now()})
               OR (status = ${Processing: EventStatus} AND execution_date < ${now() minus maxProcessingTime}))
           GROUP BY project_id
         ) oldest_event_date
         JOIN event evt ON oldest_event_date.project_id = evt.project_id 
           AND oldest_event_date.min_event_date = evt.event_date
           AND ((""" ++ `status IN`(New, RecoverableFailure) ++ fr""" AND execution_date < ${now()})
               OR (status = ${Processing: EventStatus} AND execution_date < ${now() minus maxProcessingTime})) 
         LIMIT 1
         """
    }.query[EventIdAndBody].option,
    name = "pop event - oldest"
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

  private lazy val markAsProcessing: Option[EventIdAndBody] => Free[ConnectionOp, Option[EventIdAndBody]] = {
    case None =>
      Free.pure[ConnectionOp, Option[EventIdAndBody]](None)
    case Some(idAndBody @ (commitEventId, _)) =>
      measureExecutionTime(updateStatus(commitEventId)) map toNoneIfEventAlreadyTaken(idAndBody)
  }

  private def updateStatus(commitEventId: CompoundEventId) = SqlQuery(
    sql"""|UPDATE event 
          |SET status = ${EventStatus.Processing: EventStatus}, execution_date = ${now()}
          |WHERE (event_id = ${commitEventId.id} AND project_id = ${commitEventId.projectId} AND status <> ${Processing: EventStatus})
          |  OR (event_id = ${commitEventId.id} AND project_id = ${commitEventId.projectId} AND status = ${Processing: EventStatus} AND execution_date < ${now() minus maxProcessingTime})
          |""".stripMargin.update.run,
    name = "pop event - status update"
  )

  private def toNoneIfEventAlreadyTaken(idAndBody: EventIdAndBody): Int => Option[EventIdAndBody] = {
    case 0 => None
    case 1 => Some(idAndBody)
  }

  private def maybeUpdateMetrics(maybeProject: Option[ProjectIds], maybeBody: Option[EventIdAndBody]) =
    (maybeBody, maybeProject) mapN { case (_, ProjectIds(_, projectPath)) =>
      for {
        _ <- waitingEventsGauge decrement projectPath
        _ <- underProcessingGauge increment projectPath
      } yield ()
    } getOrElse ME.unit
}

private[subscriptions] object IONewEventFetcher {

  private val MaxProcessingTime:     Duration             = Duration.ofHours(5)
  private val ProjectsFetchingLimit: Int Refined Positive = 10

  def apply(
      transactor:           DbTransactor[IO, EventLogDB],
      waitingEventsGauge:   LabeledGauge[IO, projects.Path],
      underProcessingGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:     LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift:  ContextShift[IO]): IO[EventFetcher[IO]] = IO {
    new UnprocessedEventFetcherImpl(transactor,
                                    waitingEventsGauge,
                                    underProcessingGauge,
                                    queriesExecTimes,
                                    maxProcessingTime = MaxProcessingTime,
                                    projectsFetchingLimit = ProjectsFetchingLimit,
                                    projectPrioritisation = new ProjectPrioritisation()
    )
  }
}
