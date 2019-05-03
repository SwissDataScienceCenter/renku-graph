/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import cats.data.NonEmptyList
import cats.effect.{Bracket, ContextShift, IO}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog.{EventLogDB, EventStatus}
import ch.datascience.graph.model.events.ProjectId
import doobie.implicits._
import doobie.util.fragment.Fragment
import doobie.util.fragments.in
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

  def fetchStatus(projectId: ProjectId): Interpretation[Option[ProcessingStatus]] =
    findDoneAndTotal(projectId)
      .query[(Int, Int)]
      .option
      .transact(transactor.get)
      .flatMap(toProcessingStatus)

  private def findDoneAndTotal(projectId: ProjectId): Fragment = fr"""
    select done.number, total.number 
    from (""" ++ findDone(projectId) ++ fr") done, (" ++ findTotal(projectId) ++ fr") total"

  // format: off
  private def findDone(projectId: ProjectId): Fragment =fr"""
    select count(*) as number
    from event_log
    where project_id = $projectId""" ++
      `and status IN`(TriplesStore, NonRecoverableFailure) ++ fr"""
       and created_date > ${now() minus ToBeConsideredAsBeingProcessed}"""

  private def findTotal(projectId: ProjectId): Fragment = fr"""
    select count(*) as number
    from event_log
    where project_id = $projectId 
      and created_date > ${now() minus ToBeConsideredAsBeingProcessed}"""
  // format: on

  private def `and status IN`(statuses: EventStatus*) =
    fr" and " ++ in(fr"status", NonEmptyList.fromListUnsafe(statuses.toList))

  private lazy val toProcessingStatus: Option[(Int, Int)] => Interpretation[Option[ProcessingStatus]] = {
    case None                => ME.pure(None)
    case Some((0, 0))        => ME.pure(None)
    case Some((done, total)) => ProcessingStatus.from[Interpretation](done, total) map Option.apply
  }
}

private object EventLogProcessingStatus {
  val ToBeConsideredAsBeingProcessed = Duration.of(2, MINUTES)
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
  import cats.implicits._

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
      if (total.value == 0) 100D
      else BigDecimal((done.value.toDouble / total.value) * 100).setScale(2, RoundingMode.HALF_DOWN).toDouble
    applyRef[Progress](progress) getOrError [Interpretation] s"ProcessingStatus with 'progress' $progress makes no sense"
  }
}
