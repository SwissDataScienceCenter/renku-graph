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

import java.time.temporal.ChronoUnit._
import java.time.{Duration, Instant}

import cats.MonadError
import cats.data.OptionT
import cats.effect.{Bracket, ContextShift, IO}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog.{EventLogDB, EventStatus, ExecutionDate}
import ch.datascience.graph.model.events.{CommitId, ProjectId}
import doobie.implicits._
import doobie.util.Read
import eu.timepit.refined.api.RefType.applyRef
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative

import scala.language.higherKinds
import scala.math.BigDecimal.RoundingMode

class EventLogProcessingStatus[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    now:        () => Instant = () => Instant.now
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  import EventLogProcessingStatus._

  def fetchStatus(projectId: ProjectId): OptionT[Interpretation, ProcessingStatus] =
    findOldestNotProcessed(projectId)
      .semiflatMap(toInProgressProcessingStatus(projectId))
      .orElse(toAllDoneOrNoEvents(projectId))

  private def toInProgressProcessingStatus(projectId: ProjectId)(oldestInBatch: Event) =
    for {
      events           <- findAllFromTheSameBatch(projectId, oldestInBatch)
      processingStatus <- toProcessingStatus(events)
    } yield processingStatus

  private def findOldestNotProcessed(projectId: ProjectId) = OptionT(sql"""
    select event_id, status, execution_date
    from event_log
    where project_id = $projectId
      and (status = ${EventStatus.New: EventStatus} or status = ${EventStatus.Processing: EventStatus} or status = ${EventStatus.RecoverableFailure: EventStatus})
    order by execution_date asc
    limit 1
  """.query[Event].option.transact(transactor.get))

  private def findAllFromTheSameBatch(projectId: ProjectId, oldestInBatch: Event) = sql"""
    select event_id, status, execution_date
    from event_log
    where project_id = $projectId
      and execution_date >= ${oldestInBatch.executionDate.value}
  """.query[Event].to[List].transact(transactor.get)

  private def toAllDoneOrNoEvents(projectId: ProjectId) =
    countAllEvents(projectId) flatMap {
      case 0     => OptionT.none[Interpretation, ProcessingStatus]
      case other => OptionT.liftF(ProcessingStatus.from[Interpretation](other, other))
    }

  private def countAllEvents(projectId: ProjectId) = OptionT.liftF(sql"""
    select count(*)
    from event_log
    where project_id = $projectId
  """.query[Int].unique.transact(transactor.get))

  private def toProcessingStatus(events: List[Event]) =
    events.foldLeft(0 -> 0) {
      case ((done, total), Event(_, status, _)) if status == TriplesStore || status == NonRecoverableFailure =>
        (done + 1) -> (total + 1)
      case ((done, total), _) => done -> (total + 1)
    } match {
      case (done, total) => ProcessingStatus.from(done, total)(ME)
    }
}

private object EventLogProcessingStatus {
  val MaxTimeDiffBetweenEventsInBatch = Duration.of(15, MINUTES)

  implicit val eventRead: Read[Event] = Read[(CommitId, EventStatus, ExecutionDate)].map {
    case (eventId, status, createdDate) => Event(eventId, status, createdDate)
  }

  final case class Event(eventId: CommitId, status: EventStatus, executionDate: ExecutionDate)
}

class IOEventLogProcessingStatus(
    transactor:          DbTransactor[IO, EventLogDB]
)(implicit contextShift: ContextShift[IO])
    extends EventLogProcessingStatus[IO](transactor)

import ProcessingStatus._

final case class ProcessingStatus private (
    done:     Done,
    total:    Total,
    progress: Progress
)

object ProcessingStatus {

  type Done     = Int Refined NonNegative
  type Total    = Int Refined NonNegative
  type Progress = Double Refined NonNegative

  def from[Interpretation[_]](
      done:      Int,
      total:     Int
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[ProcessingStatus] =
    for {
      validDone  <- applyRef[Done](done) getOrError [Interpretation] "ProcessingStatus's 'done' cannot be negative"
      validTotal <- applyRef[Total](total) getOrError [Interpretation] "ProcessingStatus's 'total' cannot be negative"
      _          <- checkDoneLessThanTotal[Interpretation](validDone, validTotal)
      progress   <- progressFrom[Interpretation](validDone, validTotal)
    } yield new ProcessingStatus(validDone, validTotal, progress)

  private implicit class RefTypeOps[V](maybeValue: Either[String, V]) {
    def getOrError[Interpretation[_]](
        message:   String
    )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[V] =
      maybeValue.fold(
        _ => ME.raiseError[V](new IllegalArgumentException(message)),
        ME.pure
      )
  }

  private def checkDoneLessThanTotal[Interpretation[_]](
      done:      Done,
      total:     Total
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[Unit] =
    if (done.value <= total.value) ME.unit
    else ME.raiseError(new IllegalArgumentException("ProcessingStatus with 'done' > 'total' makes no sense"))

  private def progressFrom[Interpretation[_]](
      done:      Done,
      total:     Total
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[Progress] = {
    val progress =
      if (total.value == 0) 100d
      else BigDecimal((done.value.toDouble / total.value) * 100).setScale(2, RoundingMode.HALF_DOWN).toDouble
    applyRef[Progress](progress) getOrError [Interpretation] s"ProcessingStatus with 'progress' $progress makes no sense"
  }
}
