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

package ch.datascience.dbeventlog.commands

import java.time.{Duration, Instant}

import cats.data.NonEmptyList
import cats.effect.{Bracket, ContextShift, IO}
import cats.free.Free
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog._
import ch.datascience.dbeventlog.config.RenkuLogTimeout
import ch.datascience.graph.model.events.CommitEventId
import doobie.free.connection.ConnectionOp
import doobie.implicits._
import doobie.util.fragments._

import scala.language.higherKinds
import scala.util.Random

trait EventLogFetch[Interpretation[_]] {
  def isEventToProcess:  Interpretation[Boolean]
  def popEventToProcess: Interpretation[Option[EventBody]]
}

class EventLogFetchImpl[Interpretation[_]](
    transactor:      DbTransactor[Interpretation, EventLogDB],
    renkuLogTimeout: RenkuLogTimeout,
    now:             () => Instant = () => Instant.now
)(implicit ME:       Bracket[Interpretation, Throwable])
    extends EventLogFetch[Interpretation] {

  private lazy val MaxProcessingTime = renkuLogTimeout.toUnsafe[Duration] plusMinutes 1

  override def isEventToProcess: Interpretation[Boolean] =
    findOldestEvent.transact(transactor.get).map(_.isDefined)

  override def popEventToProcess: Interpretation[Option[EventBody]] =
    findEventAndUpdateForProcessing().transact(transactor.get)

  private def findEventAndUpdateForProcessing() =
    for {
      maybeIdAndProjectAndBody <- findProjectsOldestEvents map selectRandom
      maybeBody                <- markAsProcessing(maybeIdAndProjectAndBody)
    } yield maybeBody

  // format: off
  private def findOldestEvent = {
    fr"""select event_id, project_id, event_body
         from event_log
         where (""" ++ `status IN`(New, RecoverableFailure) ++ fr""" and execution_date < ${now()})
           or (status = ${Processing: EventStatus} and execution_date < ${now() minus MaxProcessingTime})
         order by execution_date asc
         limit 1"""
  }.query[EventIdAndBody].option
  // format: on

  // format: off
  private def findProjectsOldestEvents = {
    fr"""select event_log.event_id, event_log.project_id, event_log.event_body
         from event_log, (select  project_id, min(execution_date) as oldest_execution_date
                          from event_log
                          where (""" ++ `status IN`(New, RecoverableFailure) ++ fr""" and execution_date < ${now()})
                            or (status = ${Processing: EventStatus} and execution_date < ${now() minus MaxProcessingTime})
                          group by project_id) oldest_events
         where event_log.project_id = oldest_events.project_id 
           and event_log.execution_date = oldest_events.oldest_execution_date"""
  }.query[EventIdAndBody].to[List]
  // format: on

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    in(fr"status", NonEmptyList.of(status, otherStatuses: _*))

  private lazy val selectRandom: List[EventIdAndBody] => Option[EventIdAndBody] = {
    case Nil           => None
    case single +: Nil => Some(single)
    case many          => many.get(Random.nextInt(many.size))
  }

  private lazy val markAsProcessing: Option[EventIdAndBody] => Free[ConnectionOp, Option[EventBody]] = {
    case None => Free.pure[ConnectionOp, Option[EventBody]](None)
    case Some((commitEventId, eventBody)) =>
      sql"""update event_log 
           |set status = ${EventStatus.Processing: EventStatus}, execution_date = ${now()}
           |where (event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId} and status <> ${Processing: EventStatus})
           |  or (event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId} and status = ${Processing: EventStatus} and execution_date < ${now() minus MaxProcessingTime})
           |""".stripMargin.update.run
        .map(toNoneIfEventAlreadyTaken(eventBody))
  }

  private def toNoneIfEventAlreadyTaken(eventBody: EventBody): Int => Option[EventBody] = {
    case 0 => None
    case 1 => Some(eventBody)
  }

  private type EventIdAndBody = (CommitEventId, EventBody)
}

object IOEventLogFetch {

  def apply(
      transactor:          DbTransactor[IO, EventLogDB]
  )(implicit contextShift: ContextShift[IO]): IO[EventLogFetchImpl[IO]] =
    RenkuLogTimeout[IO]() map (new EventLogFetchImpl(transactor, _))
}
