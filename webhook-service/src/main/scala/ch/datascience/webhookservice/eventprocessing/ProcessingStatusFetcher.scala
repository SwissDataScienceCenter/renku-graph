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

package ch.datascience.webhookservice.eventprocessing

import cats.MonadError
import cats.data.OptionT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.projects
import ch.datascience.http.client.IORestClient
import ch.datascience.webhookservice.eventprocessing.ProcessingStatusFetcher.ProcessingStatus
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import org.typelevel.log4cats.Logger
import io.circe.Decoder

import scala.concurrent.ExecutionContext
import scala.math.BigDecimal.RoundingMode

private trait ProcessingStatusFetcher[Interpretation[_]] {
  def fetchProcessingStatus(projectId: projects.Id): OptionT[Interpretation, ProcessingStatus]
}

private object ProcessingStatusFetcher {

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

private class IOProcessingStatusFetcher(
    eventLogUrl: EventLogUrl,
    logger:      Logger[IO]
)(implicit
    ME:               MonadError[IO, Throwable],
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(Throttler.noThrottling, logger)
    with ProcessingStatusFetcher[IO] {

  import IOProcessingStatusFetcher._
  import ProcessingStatusFetcher._
  import cats.effect._
  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._

  override def fetchProcessingStatus(projectId: projects.Id): OptionT[IO, ProcessingStatus] = OptionT {
    for {
      uri          <- validateUri(s"$eventLogUrl/processing-status") map (_.withQueryParam("project-id", projectId.toString))
      latestEvents <- send(request(GET, uri))(mapResponse)
    } yield latestEvents
  }

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[ProcessingStatus]]] = {
    case (NotFound, _, _)  => Option.empty[ProcessingStatus].pure[IO]
    case (Ok, _, response) => response.as[ProcessingStatus].map(Option.apply)
  }

  private implicit lazy val entityDecoder: EntityDecoder[IO, ProcessingStatus] = jsonOf[IO, ProcessingStatus]
}

private object IOProcessingStatusFetcher {

  import io.circe.DecodingFailure

  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ProcessingStatusFetcher[IO]] =
    for {
      eventLogUrl <- EventLogUrl[IO]()
    } yield new IOProcessingStatusFetcher(eventLogUrl, logger)

  implicit lazy val processingStatusDecoder: Decoder[ProcessingStatus] = cursor =>
    for {
      done           <- cursor.downField("done").as[Int]
      total          <- cursor.downField("total").as[Int]
      progress       <- cursor.downField("progress").as[Double]
      progressStatus <- ProcessingStatus.from(done, total, progress).leftMap(DecodingFailure(_, Nil))
    } yield progressStatus
}
