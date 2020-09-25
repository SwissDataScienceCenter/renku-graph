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

package ch.datascience.webhookservice.missedevents

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.events.{CommitId, EventBody}
import ch.datascience.graph.model.projects
import ch.datascience.http.client.IORestClient
import ch.datascience.webhookservice.missedevents.LatestEventsFetcher.LatestProjectCommit
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait LatestEventsFetcher[Interpretation[_]] {
  def fetchLatestEvents(): Interpretation[List[LatestProjectCommit]]
}

private object LatestEventsFetcher {
  final case class LatestProjectCommit(commitId: CommitId, projectId: projects.Id)
}

private class IOLatestEventsFetcher(
    eventLogUrl: EventLogUrl,
    logger:      Logger[IO]
)(implicit
    ME:               MonadError[IO, Throwable],
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(Throttler.noThrottling, logger)
    with LatestEventsFetcher[IO] {

  import IOLatestEventsFetcher.latestCommitDecoder
  import cats.effect._
  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._

  override def fetchLatestEvents(): IO[List[LatestProjectCommit]] =
    for {
      uri          <- validateUri(s"$eventLogUrl/events") map (_.withQueryParam("latest-per-project", "true"))
      latestEvents <- send(request(GET, uri))(mapResponse)
    } yield latestEvents

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[List[LatestProjectCommit]]] = {
    case (Ok, _, response) => response.as[List[LatestProjectCommit]]
  }

  private implicit lazy val entityDecoder: EntityDecoder[IO, List[LatestProjectCommit]] =
    jsonOf[IO, List[LatestProjectCommit]]
}

private object IOLatestEventsFetcher {
  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[LatestEventsFetcher[IO]] =
    for {
      eventLogUrl <- EventLogUrl[IO]()
    } yield new IOLatestEventsFetcher(eventLogUrl, logger)

  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe.Decoder

  implicit lazy val latestCommitDecoder: Decoder[LatestProjectCommit] = cursor =>
    for {
      projectId <- cursor.downField("project").downField("id").as[projects.Id]
      commitId  <- cursor.downField("body").as[EventBody].flatMap(_.decodeAs(commitId))
    } yield LatestProjectCommit(commitId, projectId)

  private val commitId: Decoder[CommitId] = _.downField("id").as[CommitId]
}
