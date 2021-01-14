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
import cats.effect.{Effect, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.subscriptions.SubscriptionMechanismRegistry
import ch.datascience.triplesgenerator.reprovisioning.ReProvisioningStatus
import io.chrisdavenport.log4cats.Logger
import org.http4s.dsl.Http4sDsl
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
  import ch.datascience.controllers.InfoMessage._
  import ch.datascience.controllers.{ErrorMessage, InfoMessage}
  import org.http4s._

  override def processEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]] =
    reProvisioningStatus.isReProvisioning() flatMap { isReProvisioning =>
      if (isReProvisioning) ServiceUnavailable(InfoMessage("Temporarily unavailable: currently re-provisioning"))
      else tryNextHandler(request, eventHandlers) flatMap toHttpResult
    }

  private def tryNextHandler(request:  Request[Interpretation],
                             handlers: List[EventHandler[Interpretation]]
  ): Interpretation[EventSchedulingResult] =
    handlers.headOption match {
      case Some(handler) =>
        handler.handle(request).flatMap {
          case UnsupportedEventType => tryNextHandler(request, handlers.tail)
          case otherResult          => otherResult.pure[Interpretation]
        }
      case None => (UnsupportedEventType: EventSchedulingResult).pure[Interpretation]
    }

  private lazy val toHttpResult: EventSchedulingResult => Interpretation[Response[Interpretation]] = {
    case EventSchedulingResult.Accepted             => Accepted(InfoMessage("Event accepted"))
    case EventSchedulingResult.Busy                 => TooManyRequests(InfoMessage("Too many events to handle"))
    case EventSchedulingResult.UnsupportedEventType => BadRequest(ErrorMessage("Unsupported Event Type"))
    case EventSchedulingResult.BadRequest           => BadRequest(ErrorMessage("Malformed event"))
    case EventSchedulingResult.SchedulingError(_)   => InternalServerError(ErrorMessage("Failed to schedule event"))
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
    } yield new EventEndpointImpl[IO](
      List(awaitingGenerationHandler),
      reProvisioningStatus
    )
}
