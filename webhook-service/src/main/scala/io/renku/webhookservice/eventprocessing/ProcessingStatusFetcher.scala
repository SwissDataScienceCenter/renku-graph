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

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.renku.control.Throttler
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.projects
import io.renku.http.client.RestClient
import org.typelevel.log4cats.Logger

private trait ProcessingStatusFetcher[F[_]] {
  def fetchProcessingStatus(projectId: projects.GitLabId): F[ProgressStatus]
}

private object ProcessingStatusFetcher {

  import io.circe.DecodingFailure

  def apply[F[_]: Async: Logger]: F[ProcessingStatusFetcher[F]] =
    EventLogUrl[F]().map(new ProcessingStatusFetcherImpl(_))

  implicit lazy val processingStatusDecoder: Decoder[ProgressStatus] = cursor =>
    for {
      done           <- cursor.downField("done").as[Int]
      total          <- cursor.downField("total").as[Int]
      progress       <- cursor.downField("progress").as[Double]
      progressStatus <- ProgressStatus.from(done, total, progress).leftMap(DecodingFailure(_, Nil))
    } yield progressStatus
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

  override def fetchProcessingStatus(projectId: projects.GitLabId): F[ProgressStatus] =
    for {
      uri <- validateUri(s"$eventLogUrl/processing-status") map (_.withQueryParam("project-id", projectId.toString))
      latestEvents <- send(request(GET, uri))(mapResponse)
    } yield latestEvents

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[ProgressStatus]] = {
    case (NotFound, _, _)  => ProgressStatus.Zero.pure[F].widen
    case (Ok, _, response) => response.as[ProgressStatus]
  }

  private implicit lazy val entityDecoder: EntityDecoder[F, ProgressStatus] = jsonOf[F, ProgressStatus]
}
