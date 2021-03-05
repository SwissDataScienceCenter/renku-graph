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

import cats.effect.{Bracket, ContextShift, IO}
import cats.free.Free
import cats.syntax.all._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples, TriplesGenerated, TriplesStore}
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import doobie.free.connection.ConnectionOp
import doobie.util.{Get, Put}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog.subscriptions.EventFinder
import io.renku.eventlog.{EventLogDB, TypeSerializers}

import java.time.{Duration, Instant}

private class LongProcessingEventFinder(transactor:             DbTransactor[IO, EventLogDB],
                                        maxProcessingTime:      EventProcessingTime,
                                        maxProcessingTimeRatio: Int Refined Positive,
                                        queriesExecTimes:       LabeledHistogram[IO, SqlQuery.Name],
                                        now:                    () => Instant = () => Instant.now
)(implicit ME:                                                  Bracket[IO, Throwable], contextShift: ContextShift[IO])
    extends DbClient(Some(queriesExecTimes))
    with EventFinder[IO, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  import doobie.implicits._

  override def popEvent(): IO[Option[ZombieEvent]] = {
    findPotentialZombies >>= lookForZombie >>= markEventTaken
  } transact transactor.get

  private def findPotentialZombies: Free[ConnectionOp, List[(projects.Id, EventStatus, EventProcessingTime)]] =
    for {
      projectsAndStatuses <- queryProjectsToCheck
      projectsAndTimes <- projectsAndStatuses.map { case (id, status) =>
                            queryProcessingTimes(id, status).map((id, status, _))
                          }.sequence
    } yield projectsAndTimes map {
      case (id, currentStatus, Nil)   => (id, currentStatus.toEventStatus, maxProcessingTime / maxProcessingTimeRatio)
      case (id, currentStatus, times) => (id, currentStatus.toEventStatus, findMedian(times))
    }

  private def findMedian(times: List[EventProcessingTime]): EventProcessingTime = {
    val median = times.sorted.reverse.apply(times.size / 2)
    if ((median.value compareTo Duration.ofSeconds(150)) < 0) EventProcessingTime(Duration.ofSeconds(150))
    else median
  }

  private def queryProjectsToCheck = measureExecutionTime {
    SqlQuery(
      sql"""|SELECT DISTINCT evt.project_id, evt.status
            |FROM event evt
            |WHERE evt.status = ${GeneratingTriples: EventStatus} 
            |  OR evt.status = ${TransformingTriples: EventStatus}
    """.stripMargin
        .query[(projects.Id, TransformationStatus)]
        .to[List],
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lpe - find projects")
    )
  }

  private def queryProcessingTimes(projectId: projects.Id, currentStatus: TransformationStatus) = measureExecutionTime {
    SqlQuery(
      sql"""|SELECT (
            |  SELECT spt.processing_time 
            |  FROM status_processing_time spt 
            |  WHERE evt.event_id = spt.event_id AND evt.project_id = spt.project_id AND evt.status = spt.status
            |)
            |FROM event evt
            |WHERE evt.project_id = $projectId AND evt.status = ${currentStatus.processingTimeFindingStatus}
            |ORDER BY evt.execution_date DESC
            |LIMIT 3
    """.stripMargin
        .query[EventProcessingTime]
        .to[List],
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lpe - find processing times")
    )
  }

  private def lookForZombie
      : List[(projects.Id, EventStatus, EventProcessingTime)] => Free[ConnectionOp, Option[ZombieEvent]] = {
    case Nil => Free.pure[ConnectionOp, Option[ZombieEvent]](None)
    case (projectId, status, processingTime) :: rest =>
      queryZombieEvent(projectId, status, processingTime).flatMap {
        case None              => lookForZombie(rest)
        case Some(zombieEvent) => Free.pure[ConnectionOp, Option[ZombieEvent]](Some(zombieEvent))
      }
  }

  private def queryZombieEvent(projectId: projects.Id, status: EventStatus, processingTime: EventProcessingTime) =
    measureExecutionTime {
      SqlQuery(
        sql"""|SELECT evt.event_id, evt.project_id, proj.project_path, evt.status
              |FROM event evt
              |JOIN project proj ON proj.project_id = evt.project_id
              |LEFT JOIN event_delivery ed ON ed.project_id = evt.project_id AND ed.event_id = evt.event_id
              |WHERE evt.project_id = $projectId 
              |  AND evt.status = $status
              |  AND (evt.message IS NULL OR evt.message <> $zombieMessage)
              |  AND (
              |    (ed.delivery_id IS NOT NULL AND (${now()} - evt.execution_date) > ${processingTime * maxProcessingTimeRatio})
              |    OR 
              |    (ed.delivery_id IS NULL AND 
              |      (${now()} - evt.execution_date) > ${EventProcessingTime(Duration.ofMinutes(5))}
              |    )
              |  )
              |LIMIT 1
              |""".stripMargin
          .query[(CompoundEventId, projects.Path, EventStatus)]
          .map { case (id, path, status) => ZombieEvent(processName, id, path, status) }
          .option,
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lpe - find")
      )
    }

  private def markEventTaken: Option[ZombieEvent] => Free[ConnectionOp, Option[ZombieEvent]] = {
    case None        => Free.pure[ConnectionOp, Option[ZombieEvent]](None)
    case Some(event) => updateMessage(event.eventId) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateMessage(eventId: CompoundEventId) = measureExecutionTime {
    SqlQuery(
      sql"""|UPDATE event
            |SET message = $zombieMessage, execution_date = ${now()}
            |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId}
            |""".stripMargin.update.run,
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lpe - update message")
    )
  }

  private def toNoneIfEventAlreadyTaken(event: ZombieEvent): Int => Option[ZombieEvent] = {
    case 0 => None
    case 1 => Some(event)
  }

  override val processName: ZombieEventProcess = ZombieEventProcess("lpe")

  private implicit val transformationStatusGet: Get[TransformationStatus] = Get[String].temap {
    case GeneratingTriples.value   => TransformationStatus.Generating.asRight[String]
    case TransformingTriples.value => TransformationStatus.Transforming.asRight[String]
    case other                     => s"${getClass.getName} cannot work with $other".asLeft[TransformationStatus]
  }
  private implicit val transformationStatusPut: Put[TransformationStatus] = Put[String].contramap(_.toEventStatus.value)

  private sealed trait TransformationStatus {
    val toEventStatus: EventStatus
    def processingTimeFindingStatus: EventStatus
  }
  private object TransformationStatus {
    final case object Generating extends TransformationStatus {
      override val toEventStatus:               EventStatus = GeneratingTriples
      override val processingTimeFindingStatus: EventStatus = TriplesGenerated
    }
    final case object Transforming extends TransformationStatus {
      override val toEventStatus:               EventStatus = TransformingTriples
      override val processingTimeFindingStatus: EventStatus = TriplesStore
    }
  }
}

private object LongProcessingEventFinder {

  private val MaxProcessingTimeRatio: Int Refined Positive = 2
  private val MaxProcessingTime:      EventProcessingTime  = EventProcessingTime(Duration ofDays 14)

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[EventFinder[IO, ZombieEvent]] = IO {
    new LongProcessingEventFinder(transactor, MaxProcessingTime, MaxProcessingTimeRatio, queriesExecTimes)
  }
}
