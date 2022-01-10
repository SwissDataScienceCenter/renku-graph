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

package io.renku.eventlog.subscriptions

import cats.MonadThrow
import cats.effect.kernel.Concurrent
import io.circe.Json
import io.renku.eventlog.subscriptions.EventProducersRegistry.{SubscriptionResult, UnsupportedPayload}
import io.renku.http.ErrorMessage
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait SubscriptionsEndpoint[F[_]] {
  def addSubscription(request: Request[F]): F[Response[F]]
}

class SubscriptionsEndpointImpl[F[_]: Concurrent: Logger](
    subscriptionCategoryRegistry: EventProducersRegistry[F]
) extends Http4sDsl[F]
    with SubscriptionsEndpoint[F] {

  import SubscriptionsEndpointImpl._
  import cats.syntax.all._
  import io.renku.http.InfoMessage
  import io.renku.http.InfoMessage._
  import org.http4s.circe._
  import org.http4s.{Request, Response}

  override def addSubscription(request: Request[F]): F[Response[F]] = {
    for {
      json         <- request.asJson recoverWith badRequest
      eitherResult <- subscriptionCategoryRegistry register json
      _            <- badRequestIfError(eitherResult)
      response     <- Accepted(InfoMessage("Subscription added"))
    } yield response
  } recoverWith httpResponse

  private lazy val badRequest: PartialFunction[Throwable, F[Json]] = { case NonFatal(exception) =>
    BadRequestError(exception).raiseError[F, Json]
  }

  private def badRequestIfError(eitherErrorSuccess: SubscriptionResult): F[Unit] =
    eitherErrorSuccess match {
      case UnsupportedPayload(message) => BadRequestError(message).raiseError[F, Unit]
      case _                           => ().pure[F]
    }

  private lazy val httpResponse: PartialFunction[Throwable, F[Response[F]]] = {
    case NotFoundError => NotFound("Category not found")
    case exception: BadRequestError =>
      BadRequest {
        Option(exception.getCause) map ErrorMessage.apply getOrElse ErrorMessage(exception.getMessage)
      }
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage("Registering subscriber failed")
      Logger[F].error(exception)(errorMessage.value)
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

  def apply[F[_]: Concurrent: Logger, T <: SubscriptionInfo](
      subscriptionCategoryRegistry: EventProducersRegistry[F]
  ): F[SubscriptionsEndpoint[F]] = MonadThrow[F].catchNonFatal {
    new SubscriptionsEndpointImpl[F](subscriptionCategoryRegistry)
  }
}
