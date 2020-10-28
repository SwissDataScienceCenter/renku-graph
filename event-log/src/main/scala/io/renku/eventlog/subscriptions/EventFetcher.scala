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

import scala.language.postfixOps
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

private trait EventFetcher[Interpretation[_]] {
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
    pickRandomlyFrom: List[(projects.Id, projects.Path)] => Option[(projects.Id, projects.Path)] = ids =>
      ids.get(Random nextInt ids.size)
)(implicit ME: Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with EventFetcher[IO] {

  import TypesSerializers._

  private type EventIdAndBody                      = (CompoundEventId, EventBody)
  private type ProjectIdAndPath                    = (projects.Id, projects.Path)
  private type ProjectIdAndPathWithLatestEventDate = (projects.Id, projects.Path, EventDate)

  override def popEvent(): IO[Option[EventIdAndBody]] =
    for {
      maybeProjectEventIdAndBody <- findEventAndUpdateForProcessing() transact transactor.get
      (maybeProject, maybeEventIdAndBody) = maybeProjectEventIdAndBody
      _ <- maybeUpdateMetrics(maybeProject, maybeEventIdAndBody)
    } yield maybeEventIdAndBody

  private def findEventAndUpdateForProcessing() =
    for {
      maybeProject <- measureExecutionTime(findProjectsWithEventsInQueue) map findWeight map selectProject
      maybeIdAndProjectAndBody <- maybeProject
                                    .map(idAndPath => measureExecutionTime(findOldestEvent(idAndPath)))
                                    .getOrElse(Free.pure[ConnectionOp, Option[EventIdAndBody]](None))
      maybeBody <- markAsProcessing(maybeIdAndProjectAndBody)
    } yield maybeProject -> maybeBody

  // format: off
  private def findProjectsWithEventsInQueue = SqlQuery({
      fr"""select distinct event_log.project_id, event_log.project_path, MAX(event_date) latest_event_date
           from (
             select distinct project_id
             from event_log
             where (""" ++ `status IN`(New, RecoverableFailure) ++ fr""" and execution_date < ${now()})
                   or (status = ${Processing: EventStatus} and execution_date < ${now() minus maxProcessingTime})
           ) projects_with_work
           join event_log on event_log.project_id = projects_with_work.project_id
           group by event_log.project_id, event_log.project_path
           order by latest_event_date desc
           limit ${projectsFetchingLimit.value}  
           """ 
    }.query[ProjectIdAndPathWithLatestEventDate].to[List],
    name = "pop event - projects"
  )
  // format: on

  // format: off
  private def findOldestEvent(idAndPath: ProjectIdAndPath) = SqlQuery({
      fr"""select event_log.event_id, event_log.project_id, event_log.event_body
           from event_log
           where project_id = ${idAndPath._1}
             and (
               (""" ++ `status IN`(New, RecoverableFailure) ++ fr""" and execution_date < ${now()})
               or (status = ${Processing: EventStatus} and execution_date < ${now() minus maxProcessingTime})
             )
           order by event_date asc
           limit 1"""
    }.query[EventIdAndBody].option,
    name = "pop event - oldest"
  )
  // format: on

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    in(fr"status", NonEmptyList.of(status, otherStatuses: _*))

  private def findWeight(projects: List[ProjectIdAndPathWithLatestEventDate]): List[(ProjectIdAndPath, BigDecimal)] = {
    def findWeight(eventDate: EventDate): BigDecimal = {
      val (_, _, latestEventDate) = projects.head
      val (_, _, oldestEventDate) = projects.last
      val maxDistance             = BigDecimal(Duration.between(oldestEventDate.value, latestEventDate.value).toHours)
      val normalisedDistance =
        if (maxDistance == 0) BigDecimal(1)
        else BigDecimal(Duration.between(oldestEventDate.value, eventDate.value).toHours.toDouble) / maxDistance
      if (normalisedDistance == 0) BigDecimal(.1)
      else normalisedDistance
    }
    projects.map { case (projectId, projectPath, eventDate) => ((projectId, projectPath), findWeight(eventDate)) }
  }

  private lazy val selectProject: List[(ProjectIdAndPath, BigDecimal)] => Option[ProjectIdAndPath] = {
    case Nil                          => None
    case (projectIdAndPath, _) +: Nil => Some(projectIdAndPath)
    case many                         => pickRandomlyFrom(weightedList(from = many))
  }

  private def weightedList(from: List[((projects.Id, projects.Path), BigDecimal)]): List[ProjectIdAndPath] =
    from.foldLeft(List.empty[ProjectIdAndPath]) { case (acc, (projectIdAndPath, weight)) =>
      acc :++ List.fill(
        (weight * 10).setScale(2, RoundingMode.HALF_UP).toInt
      )(projectIdAndPath)
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
    (maybeBody, maybeProject) mapN { case (_, (_, projectPath)) =>
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
  )(implicit contextShift:  ContextShift[IO]): IO[EventFetcher[IO]] = IO {
    new EventFetcherImpl(transactor,
                         waitingEventsGauge,
                         underProcessingGauge,
                         queriesExecTimes,
                         maxProcessingTime = MaxProcessingTime,
                         projectsFetchingLimit = ProjectsFetchingLimit
    )
  }
}
