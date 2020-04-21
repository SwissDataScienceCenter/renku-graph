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

package ch.datascience.eventlog.subscriptions

import java.time.{Duration, Instant}

import cats.data.NonEmptyList
import cats.effect.{Bracket, ContextShift, IO}
import cats.free.Free
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.eventlog.EventStatus._
import ch.datascience.eventlog._
import ch.datascience.graph.config.RenkuLogTimeout
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import doobie.free.connection.ConnectionOp
import doobie.implicits._
import doobie.util.fragments._

import scala.language.higherKinds
import scala.util.Random

private trait EventFetcher[Interpretation[_]] {
  def popEvent: Interpretation[Option[(CompoundEventId, EventBody)]]
}

private class EventFetcherImpl[Interpretation[_]](
    transactor:         DbTransactor[Interpretation, EventLogDB],
    renkuLogTimeout:    RenkuLogTimeout,
    waitingEventsGauge: LabeledGauge[Interpretation, projects.Path],
    now:                () => Instant = () => Instant.now,
    pickRandomlyFrom: List[(projects.Id, projects.Path)] => Option[(projects.Id, projects.Path)] = ids =>
      ids.get(Random nextInt ids.size)
)(implicit ME: Bracket[Interpretation, Throwable])
    extends EventFetcher[Interpretation] {

  import TypesSerializers._

  private lazy val MaxProcessingTime = renkuLogTimeout.toUnsafe[Duration] plusMinutes 5

  private type EventIdAndBody   = (CompoundEventId, EventBody)
  private type ProjectIdAndPath = (projects.Id, projects.Path)

  override def popEvent: Interpretation[Option[EventIdAndBody]] =
    for {
      maybeProjectEventIdAndBody <- findEventAndUpdateForProcessing().transact(transactor.get)
      (maybeProject, maybeEventIdAndBody) = maybeProjectEventIdAndBody
      _ <- maybeDecrementWaitingEvents(maybeProject, maybeEventIdAndBody)
    } yield maybeEventIdAndBody

  private def findEventAndUpdateForProcessing() =
    for {
      maybeProject <- findProjectsWithEventsInQueue map selectRandom
      maybeIdAndProjectAndBody <- maybeProject
                                   .map(findOldestEvent)
                                   .getOrElse(Free.pure[ConnectionOp, Option[EventIdAndBody]](None))
      maybeBody <- markAsProcessing(maybeIdAndProjectAndBody)
    } yield maybeProject -> maybeBody

  // format: off
  private def findProjectsWithEventsInQueue = {
    fr"""select distinct project_id, project_path
         from event_log
         where (""" ++ `status IN`(New, RecoverableFailure) ++ fr""" and execution_date < ${now()})
               or (status = ${Processing: EventStatus} and execution_date < ${now() minus MaxProcessingTime})"""
  }.query[ProjectIdAndPath].to[List]
  // format: on

  // format: off
  private def findOldestEvent(idAndPath: ProjectIdAndPath) = {
    fr"""select event_log.event_id, event_log.project_id, event_log.event_body
         from event_log
         where project_id = ${idAndPath._1}
           and (
             (""" ++ `status IN`(New, RecoverableFailure) ++ fr""" and execution_date < ${now()})
             or (status = ${Processing: EventStatus} and execution_date < ${now() minus MaxProcessingTime})
           )
         order by execution_date asc
         limit 1"""
  }.query[EventIdAndBody].option
  // format: on

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    in(fr"status", NonEmptyList.of(status, otherStatuses: _*))

  private lazy val selectRandom: List[ProjectIdAndPath] => Option[ProjectIdAndPath] = {
    case Nil           => None
    case single +: Nil => Some(single)
    case many          => pickRandomlyFrom(many)
  }

  private lazy val markAsProcessing: Option[EventIdAndBody] => Free[ConnectionOp, Option[EventIdAndBody]] = {
    case None => Free.pure[ConnectionOp, Option[EventIdAndBody]](None)
    case Some(idAndBody @ (commitEventId, _)) =>
      sql"""update event_log 
           |set status = ${EventStatus.Processing: EventStatus}, execution_date = ${now()}
           |where (event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId} and status <> ${Processing: EventStatus})
           |  or (event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId} and status = ${Processing: EventStatus} and execution_date < ${now() minus MaxProcessingTime})
           |""".stripMargin.update.run
        .map(toNoneIfEventAlreadyTaken(idAndBody))
  }

  private def toNoneIfEventAlreadyTaken(idAndBody: EventIdAndBody): Int => Option[EventIdAndBody] = {
    case 0 => None
    case 1 => Some(idAndBody)
  }

  private def maybeDecrementWaitingEvents(maybeProject: Option[ProjectIdAndPath], maybeBody: Option[EventIdAndBody]) =
    (maybeBody, maybeProject) mapN {
      case (_, (_, projectPath)) => waitingEventsGauge decrement projectPath
    } getOrElse ME.unit
}

private object IOEventLogFetch {

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      waitingEventsGauge:  LabeledGauge[IO, projects.Path]
  )(implicit contextShift: ContextShift[IO]): IO[EventFetcherImpl[IO]] =
    RenkuLogTimeout[IO]() map (new EventFetcherImpl(transactor, _, waitingEventsGauge))
}
