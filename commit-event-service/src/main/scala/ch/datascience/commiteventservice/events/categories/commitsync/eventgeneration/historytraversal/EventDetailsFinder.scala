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

package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal

import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.common.CommitWithParents
import ch.datascience.control.Throttler
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.http.client.RestClient
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.circe.parser.parse
import org.http4s.Status.{NotFound, Ok}
import org.http4s.circe.jsonOf
import org.http4s.{EntityDecoder, Request, Response, Status}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[eventgeneration] trait EventDetailsFinder[Interpretation[_]] {
  def checkIfExists(projectId:   projects.Id, commitId: CommitId): Interpretation[Boolean]
  def getEventDetails(projectId: projects.Id, commitId: CommitId): Interpretation[Option[CommitWithParents]]
}

private[eventgeneration] class EventDetailsFinderImpl[Interpretation[_]: ContextShift: Timer: ConcurrentEffect](
    eventLogUrl:             EventLogUrl,
    logger:                  Logger[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends RestClient[Interpretation, EventDetailsFinder[Interpretation]](Throttler.noThrottling, logger)
    with EventDetailsFinder[Interpretation] {

  import org.http4s.Method.GET

  override def checkIfExists(projectId: projects.Id, commitId: CommitId): Interpretation[Boolean] =
    fetchEventDetails(projectId, commitId)(mapResponseToBoolean)

  override def getEventDetails(projectId: projects.Id, commitId: CommitId): Interpretation[Option[CommitWithParents]] =
    fetchEventDetails(projectId, commitId)(mapResponseCommitDetails)

  private def fetchEventDetails[ResultType](projectId: projects.Id, commitId: CommitId)(
      mapResponse: PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[
        ResultType
      ]]
  ) =
    validateUri(s"$eventLogUrl/events/$commitId/$projectId") >>= (uri => send(request(GET, uri))(mapResponse))

  private lazy val mapResponseToBoolean
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Boolean]] = {
    case (Ok, _, _)       => true.pure[Interpretation]
    case (NotFound, _, _) => false.pure[Interpretation]
  }

  private lazy val mapResponseCommitDetails
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[
        Option[CommitWithParents]
      ]] = {
    case (Ok, _, response) => response.as[CommitWithParents].map(_.some)
    case (NotFound, _, _)  => Option.empty[CommitWithParents].pure[Interpretation]
  }

  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  private implicit val commitDetailsEntityDecoder: EntityDecoder[Interpretation, CommitWithParents] = {
    implicit val commitDecoder: Decoder[CommitWithParents] = cursor =>
      for {
        id         <- cursor.downField("id").as[CommitId]
        projectId  <- cursor.downField("project").downField("id").as[projects.Id]
        bodyString <- cursor.downField("body").as[String]
        parents = parse(bodyString)
                    .map(eventBodyJson =>
                      eventBodyJson.hcursor.downField("parents").as[List[CommitId]].getOrElse(List.empty[CommitId])
                    )
                    .getOrElse(List.empty[CommitId])
      } yield CommitWithParents(id, projectId, parents)
    jsonOf[Interpretation, CommitWithParents]
  }

}

private[eventgeneration] object EventDetailsFinder {
  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventDetailsFinderImpl[IO]] = for {
    eventLogUrl <- EventLogUrl[IO]()
  } yield new EventDetailsFinderImpl(eventLogUrl, logger)
}
