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

import cats.MonadError
import cats.data.{Kleisli, OptionT}
import cats.effect.{Async, Bracket, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.projects.Id
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.RefType.applyRef
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.eventlog.EventLogDB
import skunk._
import skunk.implicits._
import skunk.codec.all._

import scala.math.BigDecimal.RoundingMode

trait ProcessingStatusFinder[Interpretation[_]] {
  def fetchStatus(projectId: Id): OptionT[Interpretation, ProcessingStatus]
}

class ProcessingStatusFinderImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name]
) extends DbClient(Some(queriesExecTimes))
    with ProcessingStatusFinder[Interpretation] {

  import eu.timepit.refined.auto._
  import io.renku.eventlog.TypeSerializers._

  override def fetchStatus(projectId: Id): OptionT[Interpretation, ProcessingStatus] = OptionT {
    sessionResource.useK {
      measureExecutionTime(latestBatchStatues(projectId))
    } flatMap toProcessingStatus
  }

  private def latestBatchStatues(projectId: Id) = SqlQuery[Interpretation, List[EventStatus]](
    Kleisli { session =>
      val query: Query[Id ~ Id, EventStatus] =
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
      session.prepare(query).use(pq => pq.stream(projectId ~ projectId, chunkSize = 32).compile.toList)
    },
    name = "processing status"
  )

  private def toProcessingStatus(statuses: List[EventStatus]) =
    statuses.foldLeft(0 -> 0) {
      case ((done, total), _: FinalStatus) =>
        (done + 1) -> (total + 1)
      case ((done, total), _) => done -> (total + 1)
    } match {
      case (0, 0)        => Option.empty[ProcessingStatus].pure[Interpretation]
      case (done, total) => ProcessingStatus.from[Interpretation](done, total) map Option.apply
    }
}

object IOProcessingStatusFinder {
  def apply(
      sessionResource:     SessionResource[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[ProcessingStatusFinder[IO]] = IO {
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

  def from[Interpretation[_]: MonadError[*[_], Throwable]](
      done:  Int,
      total: Int
  ): Interpretation[ProcessingStatus] =
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
    applyRef[Progress](
      progress
    ) getOrError [Interpretation] s"ProcessingStatus with 'progress' $progress makes no sense"
  }
}
