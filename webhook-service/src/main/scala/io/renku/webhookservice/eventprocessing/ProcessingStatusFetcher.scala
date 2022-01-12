/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.webhookservice.eventprocessing

import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.renku.control.Throttler
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.projects
import io.renku.http.client.RestClient
import io.renku.webhookservice.eventprocessing.ProcessingStatusFetcher.ProcessingStatus
import org.typelevel.log4cats.Logger

import scala.math.BigDecimal.RoundingMode

private trait ProcessingStatusFetcher[F[_]] {
  def fetchProcessingStatus(projectId: projects.Id): OptionT[F, ProcessingStatus]
}

private object ProcessingStatusFetcher {

  import io.circe.DecodingFailure

  def apply[F[_]: Async: Logger]: F[ProcessingStatusFetcher[F]] =
    for {
      eventLogUrl <- EventLogUrl[F]()
    } yield new ProcessingStatusFetcherImpl(eventLogUrl)

  implicit lazy val processingStatusDecoder: Decoder[ProcessingStatus] = cursor =>
    for {
      done           <- cursor.downField("done").as[Int]
      total          <- cursor.downField("total").as[Int]
      progress       <- cursor.downField("progress").as[Double]
      progressStatus <- ProcessingStatus.from(done, total, progress).leftMap(DecodingFailure(_, Nil))
    } yield progressStatus

  import ProcessingStatus._

  case class ProcessingStatus private (
      done:     Done,
      total:    Total,
      progress: Progress
  )

  object ProcessingStatus {

    type Done     = Int Refined NonNegative
    type Total    = Int Refined NonNegative
    type Progress = Double Refined NonNegative

    def from(
        done:     Int,
        total:    Int,
        progress: Double
    ): Either[String, ProcessingStatus] = {
      import eu.timepit.refined.api.RefType.applyRef

      for {
        validDone     <- applyRef[Done](done).leftMap(_ => "ProcessingStatus's 'done' cannot be negative")
        validTotal    <- applyRef[Total](total).leftMap(_ => "ProcessingStatus's 'total' cannot be negative")
        validProgress <- applyRef[Progress](progress).leftMap(_ => "ProcessingStatus's 'progress' cannot be negative")
        _             <- if (done <= total) Right(()) else Left("ProcessingStatus's 'done' > 'total'")
        expectedProgress = BigDecimal((done.toDouble / total) * 100).setScale(2, RoundingMode.HALF_DOWN).toDouble
        _ <- if (expectedProgress == progress) Right(()) else Left("ProcessingStatus's 'progress' is invalid")
      } yield new ProcessingStatus(validDone, validTotal, validProgress)
    }
  }
}

private class ProcessingStatusFetcherImpl[F[_]: Async: Logger](
    eventLogUrl: EventLogUrl
) extends RestClient[F, ProcessingStatusFetcher[F]](Throttler.noThrottling)
    with ProcessingStatusFetcher[F] {

  import ProcessingStatusFetcher._
  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._

  override def fetchProcessingStatus(projectId: projects.Id): OptionT[F, ProcessingStatus] =
    OptionT {
      for {
        uri <- validateUri(s"$eventLogUrl/processing-status") map (_.withQueryParam("project-id", projectId.toString))
        latestEvents <- send(request(GET, uri))(mapResponse)
      } yield latestEvents
    }

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[ProcessingStatus]]] = {
    case (NotFound, _, _)  => Option.empty[ProcessingStatus].pure[F]
    case (Ok, _, response) => response.as[ProcessingStatus].map(Option.apply)
  }

  private implicit lazy val entityDecoder: EntityDecoder[F, ProcessingStatus] =
    jsonOf[F, ProcessingStatus]
}
