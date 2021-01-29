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
import cats.effect.{Effect, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.IOEventEndpoint.EventRequestContent
import ch.datascience.triplesgenerator.events.subscriptions.SubscriptionMechanismRegistry
import ch.datascience.triplesgenerator.reprovisioning.ReProvisioningStatus
import fs2.text.utf8Decode
import io.chrisdavenport.log4cats.Logger
import io.circe.Json
import io.circe.parser.parse
import org.http4s.dsl.Http4sDsl
import org.http4s.multipart.Multipart
import org.http4s.{Request, Response}

import scala.concurrent.ExecutionContext

trait EventEndpoint[Interpretation[_]] {
  def processEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]]
}

class EventEndpointImpl[Interpretation[_]: Effect](
    eventHandlers:        List[EventHandler[Interpretation]],
    reProvisioningStatus: ReProvisioningStatus[Interpretation]
)(implicit ME:            MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation]
    with EventEndpoint[Interpretation] {

  import EventSchedulingResult._
  import cats.syntax.all._
  import ch.datascience.http.InfoMessage._
  import ch.datascience.http.InfoMessage
  import org.http4s._

  override def processEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]] =
    reProvisioningStatus.isReProvisioning() flatMap { isReProvisioning =>
      if (isReProvisioning) ServiceUnavailable(InfoMessage("Temporarily unavailable: currently re-provisioning"))
      else decodeRequestAndProcess(request)
    }

  private def tryNextHandler(requestContent: EventRequestContent,
                             handlers:       List[EventHandler[Interpretation]]
  ): Interpretation[EventSchedulingResult] =
    handlers.headOption match {
      case Some(handler) =>
        handler.handle(requestContent).flatMap {
          case UnsupportedEventType => tryNextHandler(requestContent, handlers.tail)
          case otherResult          => otherResult.pure[Interpretation]
        }
      case None =>
        (UnsupportedEventType: EventSchedulingResult).pure[Interpretation]
    }

  private lazy val toHttpResult: EventSchedulingResult => Interpretation[Response[Interpretation]] = {
    case EventSchedulingResult.Accepted             => Accepted(InfoMessage("Event accepted"))
    case EventSchedulingResult.Busy                 => TooManyRequests(InfoMessage("Too many events to handle"))
    case EventSchedulingResult.UnsupportedEventType => BadRequest(ErrorMessage("Unsupported Event Type"))
    case EventSchedulingResult.BadRequest           => BadRequest(ErrorMessage("Malformed event"))
    case EventSchedulingResult.SchedulingError(_)   => InternalServerError(ErrorMessage("Failed to schedule event"))
  }

  private def decodeRequestAndProcess(
      request: Request[Interpretation]
  )(implicit
      ME: MonadError[Interpretation, Throwable]
  ): Interpretation[Response[Interpretation]] =
    request.decode[Multipart[Interpretation]] { p =>
      (p.parts.find(_.name.contains("event")), p.parts.find(_.name.contains("payload"))) match {
        case (Some(eventPart), maybePayloadPart) =>
          {
            for {
              eventStr <- EitherT.liftF[Interpretation, Response[Interpretation], String](
                            eventPart.body.through(utf8Decode).compile.foldMonoid
                          )
              event <- EitherT
                         .fromEither[Interpretation](parse(eventStr))
                         .leftSemiflatMap(_ => toHttpResult(EventSchedulingResult.BadRequest))
              maybePayload <- EitherT.liftF[Interpretation, Response[Interpretation], Option[String]](
                                maybePayloadPart.map(_.body.through(utf8Decode).compile.foldMonoid).sequence
                              )
              eventRequestContent = EventRequestContent(event, maybePayload)
              result <- EitherT
                          .liftF[Interpretation, Response[Interpretation], EventSchedulingResult](
                            tryNextHandler(eventRequestContent, eventHandlers)
                          )
              httpResult <-
                EitherT
                  .liftF[Interpretation, Response[Interpretation], Response[Interpretation]](toHttpResult(result))
            } yield httpResult
          }.merge
        case _ => toHttpResult(EventSchedulingResult.BadRequest)
      }
    }
}

object IOEventEndpoint {
  import cats.effect.{ContextShift, IO}

  def apply(
      currentVersionPair:            RenkuVersionPair,
      metricsRegistry:               MetricsRegistry[IO],
      gitLabThrottler:               Throttler[IO, GitLab],
      timeRecorder:                  SparqlQueryTimeRecorder[IO],
      subscriptionMechanismRegistry: SubscriptionMechanismRegistry[IO],
      reProvisioningStatus:          ReProvisioningStatus[IO],
      logger:                        Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[EventEndpoint[IO]] =
    for {
      awaitingGenerationHandler <- categories.awaitinggeneration.EventHandler(currentVersionPair,
                                                                              metricsRegistry,
                                                                              gitLabThrottler,
                                                                              timeRecorder,
                                                                              subscriptionMechanismRegistry,
                                                                              logger
                                   )

      membersSyncHandler <- categories.membersync.EventHandler(gitLabThrottler, logger, timeRecorder)
      triplesGeneratedHandler <- categories.triplesgenerated.EventHandler(metricsRegistry,
                                                                          gitLabThrottler,
                                                                          timeRecorder,
                                                                          subscriptionMechanismRegistry,
                                                                          logger
                                 )
    } yield new EventEndpointImpl[IO](
      List(awaitingGenerationHandler, membersSyncHandler, triplesGeneratedHandler),
      reProvisioningStatus
    )

  case class EventRequestContent(event: Json, maybePayload: Option[String])
}
