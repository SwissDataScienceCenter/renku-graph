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

package io.renku.eventlog.subscriptions

import java.time.{Duration, Instant}

import cats.data.NonEmptyList
import cats.effect.{Bracket, ContextShift, IO}
import cats.free.Free
import cats.syntax.all._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import doobie.free.connection.ConnectionOp
import doobie.implicits._
import doobie.util.fragments._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog.EventStatus._
import io.renku.eventlog._
import io.renku.eventlog.subscriptions.ProjectPrioritisation.{Priority, ProjectIdAndPath, ProjectInfo}

import scala.language.postfixOps
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

private trait EventFetcher[Interpretation[_]] {
  def createView: Interpretation[Unit]
  def popEvent(): Interpretation[Option[(CompoundEventId, EventBody)]]
}

private class EventFetcherImpl(
    transactor:            DbTransactor[IO, EventLogDB],
    waitingEventsGauge:    LabeledGauge[IO, projects.Path],
    underProcessingGauge:  LabeledGauge[IO, projects.Path],
    queriesExecTimes:      LabeledHistogram[IO, SqlQuery.Name],
    now:                   () => Instant = () => Instant.now,
    maxProcessingTime:     Duration,
    projectsFetchingLimit: Int Refined Positive,
    projectPrioritisation: ProjectPrioritisation,
    pickRandomlyFrom:      List[ProjectIdAndPath] => Option[ProjectIdAndPath] = ids => ids.get(Random nextInt ids.size),
    waitForViewRefresh:    Boolean = false
)(implicit ME:             Bracket[IO, Throwable], contextShift: ContextShift[IO])
    extends DbClient(Some(queriesExecTimes))
    with EventFetcher[IO]
    with TypesSerializers {

  private type EventIdAndBody = (CompoundEventId, EventBody)

  override def createView: IO[Unit] = {
    for {
      _ <- createProjectLatestEventDateView.update.run
      _ <- createIndexForView.update.run
    } yield ()
  } transact transactor.get

  private lazy val createProjectLatestEventDateView = sql"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS project_latest_event_date AS
      select
        project_id,
        project_path,
        MAX(event_date) latest_event_date
      from event_log
      group by project_id, project_path
      order by latest_event_date desc;
    """

  private lazy val createIndexForView = sql"""
    CREATE UNIQUE INDEX IF NOT EXISTS project_latest_event_date_project_idx 
    ON project_latest_event_date (project_id, project_path)
    """

  override def popEvent(): IO[Option[EventIdAndBody]] =
    for {
      _                          <- refreshProjectsView
      maybeProjectEventIdAndBody <- findEventAndUpdateForProcessing() transact transactor.get
      (maybeProject, maybeEventIdAndBody) = maybeProjectEventIdAndBody
      _ <- maybeUpdateMetrics(maybeProject, maybeEventIdAndBody)
    } yield maybeEventIdAndBody

  private lazy val refreshProjectsView: IO[Unit] = {
    val refreshUpdate = sql"""
      REFRESH MATERIALIZED VIEW CONCURRENTLY project_latest_event_date
    """.update.run transact transactor.get
    if (waitForViewRefresh) refreshUpdate.void else refreshUpdate.start.void
  }

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
      select 
        pled.project_id,
        pled.project_path,
        pled.latest_event_date,
        (select count(event_id) from event_log el_int where el_int.project_id = pled.project_id and el_int.status = ${Processing: EventStatus}) as current_occupancy 
      from project_latest_event_date pled
      where exists (
        select project_id
        from event_log el
        where el.project_id = pled.project_id
          and ((""" ++ `status IN`(New, RecoverableFailure) ++ fr""" and execution_date < ${now()})
            or (status = ${Processing: EventStatus} and execution_date < ${now() minus maxProcessingTime})
          )
      )
      limit ${projectsFetchingLimit.value}  
      """ 
    }.query[(projects.Id, projects.Path, EventDate, Int)]
    .map { case (projectId, projectPath, eventDate, currentOccupancy) => ProjectInfo(projectId, projectPath, eventDate, Refined.unsafeApply(currentOccupancy)) }
    .to[List],
    name = "pop event - projects"
  )
  // format: on

  // format: off
  private def findOldestEvent(idAndPath: ProjectIdAndPath) = SqlQuery({
      fr"""select el.event_id, el.project_id, el.event_body
           from (
             select project_id, min(event_date) as min_event_date
             from event_log
             where project_id = ${idAndPath.id}
               and ((""" ++ `status IN`(New, RecoverableFailure) ++ fr""" and execution_date < ${now()})
                 or (status = ${Processing: EventStatus} and execution_date < ${now() minus maxProcessingTime}))
             group by project_id
           ) oldest_event_date
           join event_log el on oldest_event_date.project_id = el.project_id and oldest_event_date.min_event_date = el.event_date
           limit 1
           """
    }.query[EventIdAndBody].option,
    name = "pop event - oldest"
  )
  // format: on

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    in(fr"status", NonEmptyList.of(status, otherStatuses: _*))

  private lazy val selectProject: List[(ProjectIdAndPath, Priority)] => Option[ProjectIdAndPath] = {
    case Nil                          => None
    case (projectIdAndPath, _) +: Nil => Some(projectIdAndPath)
    case many                         => pickRandomlyFrom(prioritiesList(from = many))
  }

  private def prioritiesList(from: List[(ProjectIdAndPath, Priority)]): List[ProjectIdAndPath] =
    from.foldLeft(List.empty[ProjectIdAndPath]) { case (acc, (projectIdAndPath, priority)) =>
      acc :++ List.fill((priority.value * 10).setScale(2, RoundingMode.HALF_UP).toInt)(projectIdAndPath)
    }

  private lazy val markAsProcessing: Option[EventIdAndBody] => Free[ConnectionOp, Option[EventIdAndBody]] = {
    case None =>
      Free.pure[ConnectionOp, Option[EventIdAndBody]](None)
    case Some(idAndBody @ (commitEventId, _)) =>
      measureExecutionTime(updateStatus(commitEventId)) map toNoneIfEventAlreadyTaken(idAndBody)
  }

  private def updateStatus(commitEventId: CompoundEventId) = SqlQuery(
    sql"""|update event_log 
          |set status = ${EventStatus.Processing: EventStatus}, execution_date = ${now()}
          |where (event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId} and status <> ${Processing: EventStatus})
          |  or (event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId} and status = ${Processing: EventStatus} and execution_date < ${now() minus maxProcessingTime})
          |""".stripMargin.update.run,
    name = "pop event - status update"
  )

  private def toNoneIfEventAlreadyTaken(idAndBody: EventIdAndBody): Int => Option[EventIdAndBody] = {
    case 0 => None
    case 1 => Some(idAndBody)
  }

  private def maybeUpdateMetrics(maybeProject: Option[ProjectIdAndPath], maybeBody: Option[EventIdAndBody]) =
    (maybeBody, maybeProject) mapN { case (_, ProjectIdAndPath(_, projectPath)) =>
      for {
        _ <- waitingEventsGauge decrement projectPath
        _ <- underProcessingGauge increment projectPath
      } yield ()
    } getOrElse ME.unit
}

private object IOEventLogFetch {

  private val MaxProcessingTime:     Duration             = Duration.ofMinutes(120)
  private val ProjectsFetchingLimit: Int Refined Positive = 10

  def apply(
      transactor:           DbTransactor[IO, EventLogDB],
      waitingEventsGauge:   LabeledGauge[IO, projects.Path],
      underProcessingGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:     LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift:  ContextShift[IO]): IO[EventFetcher[IO]] = for {
    fetcher <- IO {
                 new EventFetcherImpl(transactor,
                                      waitingEventsGauge,
                                      underProcessingGauge,
                                      queriesExecTimes,
                                      maxProcessingTime = MaxProcessingTime,
                                      projectsFetchingLimit = ProjectsFetchingLimit,
                                      projectPrioritisation = new ProjectPrioritisation()
                 )
               }
    _ <- fetcher.createView
  } yield fetcher
}
