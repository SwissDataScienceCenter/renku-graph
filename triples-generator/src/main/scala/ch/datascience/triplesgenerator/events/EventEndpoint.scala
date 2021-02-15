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

package ch.datascience.triplesgenerator.events

import cats.MonadError
import cats.data.EitherT
import cats.data.EitherT.right
import cats.effect.{Effect, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.http.ErrorMessage
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.reprovisioning.ReProvisioningStatus
import io.chrisdavenport.log4cats.Logger
import io.circe.Json
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.multipart.Multipart
import org.http4s.{Request, Response}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait EventEndpoint[Interpretation[_]] {
  def processEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]]
}

class EventEndpointImpl[Interpretation[_]: Effect](
    handlersRegistry:     SubscriptionsRegistry[Interpretation],
    reProvisioningStatus: ReProvisioningStatus[Interpretation]
)(implicit ME:            MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation]
    with EventEndpoint[Interpretation] {

  import cats.syntax.all._
  import ch.datascience.http.InfoMessage
  import ch.datascience.http.InfoMessage._
  import org.http4s._

  override def processEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]] =
    reProvisioningStatus.isReProvisioning() flatMap { isReProvisioning =>
      if (isReProvisioning) ServiceUnavailable(InfoMessage("Temporarily unavailable: currently re-provisioning"))
      else {
        for {
          multipart    <- toMultipart(request)
          eventJson    <- toEvent(multipart)
          maybePayload <- getPayload(multipart)
          result <-
            right[Response[Interpretation]](
              handlersRegistry.handle(EventRequestContent(eventJson, maybePayload)) >>= toHttpResult
            )
        } yield result
      }.merge recoverWith { case NonFatal(error) =>
        toHttpResult(EventSchedulingResult.SchedulingError(error))
      }
    }

  private lazy val toHttpResult: EventSchedulingResult => Interpretation[Response[Interpretation]] = {
    case EventSchedulingResult.Accepted             => Accepted(InfoMessage("Event accepted"))
    case EventSchedulingResult.Busy                 => TooManyRequests(InfoMessage("Too many events to handle"))
    case EventSchedulingResult.UnsupportedEventType => BadRequest(ErrorMessage("Unsupported Event Type"))
    case EventSchedulingResult.BadRequest           => BadRequest(ErrorMessage("Malformed event"))
    case EventSchedulingResult.SchedulingError(_)   => InternalServerError(ErrorMessage("Failed to schedule event"))
  }

  private def toMultipart(
      request: Request[Interpretation]
  ): EitherT[Interpretation, Response[Interpretation], Multipart[Interpretation]] = EitherT {
    request.as[Multipart[Interpretation]].map(_.asRight[Response[Interpretation]]) recoverWith { case NonFatal(_) =>
      BadRequest(ErrorMessage("Not multipart request")).map(_.asLeft[Multipart[Interpretation]])
    }
  }

  private def toEvent(multipart: Multipart[Interpretation]): EitherT[Interpretation, Response[Interpretation], Json] =
    EitherT {
      multipart.parts
        .find(_.name.contains("event"))
        .map(_.as[Json].map(_.asRight[Response[Interpretation]]).recoverWith { case NonFatal(_) =>
          BadRequest(ErrorMessage("Malformed event body")).map(_.asLeft[Json])
        })
        .getOrElse(BadRequest(ErrorMessage("Missing event part")).map(_.asLeft[Json]))
    }

  private def getPayload(
      multipart: Multipart[Interpretation]
  ): EitherT[Interpretation, Response[Interpretation], Option[String]] = EitherT {
    multipart.parts
      .find(_.name.contains("payload"))
      .map {
        _.as[String].map(_.some.asRight[Response[Interpretation]]).recoverWith { case NonFatal(_) =>
          BadRequest(ErrorMessage("Malformed event payload")).map(_.asLeft[Option[String]])
        }
      }
      .getOrElse(Option.empty[String].asRight[Response[Interpretation]].pure[Interpretation])
  }

}

object IOEventEndpoint {
  import cats.effect.{ContextShift, IO}

  def apply(
      currentVersionPair:    RenkuVersionPair,
      metricsRegistry:       MetricsRegistry[IO],
      gitLabThrottler:       Throttler[IO, GitLab],
      timeRecorder:          SparqlQueryTimeRecorder[IO],
      subscriptionsRegistry: SubscriptionsRegistry[IO],
      reProvisioningStatus:  ReProvisioningStatus[IO],
      logger:                Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventEndpoint[IO]] = IO(new EventEndpointImpl[IO](subscriptionsRegistry, reProvisioningStatus))

}
