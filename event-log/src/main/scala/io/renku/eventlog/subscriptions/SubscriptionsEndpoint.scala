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

package io.renku.eventlog.subscriptions

import cats.MonadThrow
import cats.effect.Effect
import ch.datascience.http.ErrorMessage
import io.circe.Json
import io.renku.eventlog.subscriptions.EventProducersRegistry.{SubscriptionResult, UnsupportedPayload}
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait SubscriptionsEndpoint[Interpretation[_]] {
  def addSubscription(request: Request[Interpretation]): Interpretation[Response[Interpretation]]
}

class SubscriptionsEndpointImpl[Interpretation[_]: Effect: MonadThrow](
    subscriptionCategoryRegistry: EventProducersRegistry[Interpretation],
    logger:                       Logger[Interpretation]
) extends Http4sDsl[Interpretation]
    with SubscriptionsEndpoint[Interpretation] {

  import SubscriptionsEndpointImpl._
  import cats.syntax.all._
  import ch.datascience.http.InfoMessage
  import ch.datascience.http.InfoMessage._
  import org.http4s.circe._
  import org.http4s.{Request, Response}

  override def addSubscription(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      json         <- request.asJson recoverWith badRequest
      eitherResult <- subscriptionCategoryRegistry register json
      _            <- badRequestIfError(eitherResult)
      response     <- Accepted(InfoMessage("Subscription added"))
    } yield response
  } recoverWith httpResponse

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[Json]] = { case NonFatal(exception) =>
    BadRequestError(exception).raiseError[Interpretation, Json]
  }

  private def badRequestIfError(eitherErrorSuccess: SubscriptionResult): Interpretation[Unit] =
    eitherErrorSuccess match {
      case UnsupportedPayload(message) => BadRequestError(message).raiseError[Interpretation, Unit]
      case _                           => ().pure[Interpretation]
    }

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NotFoundError => NotFound("Category not found")
    case exception: BadRequestError =>
      BadRequest {
        Option(exception.getCause) map ErrorMessage.apply getOrElse ErrorMessage(exception.getMessage)
      }
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage("Registering subscriber failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }
}

private object SubscriptionsEndpointImpl {

  private case object NotFoundError extends Throwable

  private sealed trait BadRequestError extends Throwable
  private object BadRequestError {
    def apply(message: String): BadRequestError = new Exception(message) with BadRequestError

    def apply(cause: Throwable): BadRequestError = new Exception(cause) with BadRequestError
  }
}

object SubscriptionsEndpoint {

  import cats.effect.{ContextShift, IO}

  def apply[T <: SubscriptionInfo](
      subscriptionCategoryRegistry: EventProducersRegistry[IO],
      logger:                       Logger[IO]
  )(implicit contextShift:          ContextShift[IO]): IO[SubscriptionsEndpoint[IO]] = IO {
    new SubscriptionsEndpointImpl[IO](subscriptionCategoryRegistry, logger)
  }
}
