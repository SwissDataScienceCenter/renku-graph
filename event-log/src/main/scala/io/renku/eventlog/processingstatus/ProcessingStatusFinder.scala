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

package io.renku.eventlog.processingstatus

import cats.MonadThrow
import cats.data.OptionT
import cats.effect.MonadCancelThrow
import cats.effect.kernel.Async
import cats.syntax.all._
import eu.timepit.refined.api.RefType.applyRef
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.db.implicits._
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.eventlog.EventLogDB
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects.Id
import io.renku.metrics.LabeledHistogram
import skunk._
import skunk.implicits._

import scala.math.BigDecimal.RoundingMode

trait ProcessingStatusFinder[F[_]] {
  def fetchStatus(projectId: Id): OptionT[F, ProcessingStatus]
}

class ProcessingStatusFinderImpl[F[_]: MonadCancelThrow: Async](
    sessionResource:  SessionResource[F, EventLogDB],
    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
) extends DbClient(Some(queriesExecTimes))
    with ProcessingStatusFinder[F] {

  import eu.timepit.refined.auto._
  import io.renku.eventlog.TypeSerializers._

  override def fetchStatus(projectId: Id): OptionT[F, ProcessingStatus] = OptionT {
    sessionResource.useK(measureExecutionTime(latestBatchStatues(projectId))) >>= toProcessingStatus
  }

  private def latestBatchStatues(projectId: Id) = SqlStatement[F](name = "processing status")
    .select[Id ~ Id, EventStatus](
      sql"""SELECT evt.status
            FROM event evt
            INNER JOIN (
              SELECT batch_date
              FROM event
              WHERE project_id = $projectIdEncoder
              ORDER BY batch_date DESC
              LIMIT 1
            ) max_batch_date ON evt.batch_date = max_batch_date.batch_date
            WHERE evt.project_id = $projectIdEncoder
           """.query(eventStatusDecoder)
    )
    .arguments(projectId ~ projectId)
    .build(_.toList)

  private def toProcessingStatus(statuses: List[EventStatus]) =
    statuses.foldLeft(0 -> 0) {
      case ((done, total), _: FinalStatus) =>
        (done + 1) -> (total + 1)
      case ((done, total), _) => done -> (total + 1)
    } match {
      case (0, 0)        => Option.empty[ProcessingStatus].pure[F]
      case (done, total) => ProcessingStatus.from[F](done, total) map Option.apply
    }
}

object ProcessingStatusFinder {
  def apply[F[_]: MonadCancelThrow: Async](
      sessionResource:  SessionResource[F, EventLogDB],
      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[ProcessingStatusFinder[F]] = MonadThrow[F].catchNonFatal {
    new ProcessingStatusFinderImpl(sessionResource, queriesExecTimes)
  }
}

import io.renku.eventlog.processingstatus.ProcessingStatus._

final case class ProcessingStatus private (
    done:     Done,
    total:    Total,
    progress: Progress
)

object ProcessingStatus {

  type Done     = Int Refined NonNegative
  type Total    = Int Refined NonNegative
  type Progress = Double Refined NonNegative

  def from[F[_]: MonadThrow](done: Int, total: Int): F[ProcessingStatus] = for {
    validDone  <- applyRef[Done](done) getOrError [F] "ProcessingStatus's 'done' cannot be negative"
    validTotal <- applyRef[Total](total) getOrError [F] "ProcessingStatus's 'total' cannot be negative"
    _          <- checkDoneLessThanTotal[F](validDone, validTotal)
    progress   <- progressFrom[F](validDone, validTotal)
  } yield new ProcessingStatus(validDone, validTotal, progress)

  private implicit class RefTypeOps[V](maybeValue: Either[String, V]) {
    def getOrError[F[_]: MonadThrow](
        message: String
    ): F[V] = maybeValue.fold(
      _ => MonadThrow[F].raiseError[V](new IllegalArgumentException(message)),
      MonadThrow[F].pure
    )
  }

  private def checkDoneLessThanTotal[F[_]: MonadThrow](
      done:  Done,
      total: Total
  ): F[Unit] =
    if (done.value <= total.value) MonadThrow[F].unit
    else
      MonadThrow[F].raiseError(
        new IllegalArgumentException("ProcessingStatus with 'done' > 'total' makes no sense")
      )

  private def progressFrom[F[_]: MonadThrow](done: Done, total: Total): F[Progress] = {
    val progress =
      if (total.value == 0) 100d
      else BigDecimal((done.value.toDouble / total.value) * 100).setScale(2, RoundingMode.HALF_DOWN).toDouble
    applyRef[Progress](
      progress
    ) getOrError [F] s"ProcessingStatus with 'progress' $progress makes no sense"
  }
}
