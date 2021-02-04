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

import cats.MonadError
import cats.effect.Effect
import ch.datascience.http.ErrorMessage
import io.chrisdavenport.log4cats.Logger
import io.circe.Json
import io.renku.eventlog.subscriptions.SubscriptionCategoryRegistry.{SubscriptionResult, UnsupportedPayload}
import org.http4s.dsl.Http4sDsl

import scala.util.control.NonFatal

class SubscriptionsEndpoint[Interpretation[_]: Effect](
    subscriptionCategoryRegistry: SubscriptionCategoryRegistry[Interpretation],
    logger:                       Logger[Interpretation]
)(implicit ME:                    MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import SubscriptionsEndpoint._
  import cats.syntax.all._
  import ch.datascience.http.InfoMessage
  import ch.datascience.http.InfoMessage._
  import org.http4s.circe._
  import org.http4s.{Request, Response}

  def addSubscription(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      json         <- request.asJson recoverWith badRequest
      eitherResult <- subscriptionCategoryRegistry.register(json)
      _            <- badRequestIfError(eitherResult)
      response     <- Accepted(InfoMessage("Subscription added"))
    } yield response
  } recoverWith httpResponse

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[Json]] = { case NonFatal(exception) =>
    ME.raiseError(BadRequestError(exception))
  }

  private def badRequestIfError(eitherErrorSuccess: SubscriptionResult): Interpretation[Unit] =
    eitherErrorSuccess match {
      case UnsupportedPayload(message) => ME.raiseError[Unit](BadRequestError(message))
      case _                           => ME.unit
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

object SubscriptionsEndpoint {

  private sealed trait BadRequestError extends Throwable

  private object BadRequestError {
    def apply(message: String): BadRequestError = new Exception(message) with BadRequestError

    def apply(cause: Throwable): BadRequestError = new Exception(cause) with BadRequestError
  }

  private case object NotFoundError extends Throwable

}

object IOSubscriptionsEndpoint {

  import cats.effect.{ContextShift, IO}

  def apply[T <: SubscriptionCategoryPayload](
      subscriptionCategoryRegistry: SubscriptionCategoryRegistry[IO],
      logger:                       Logger[IO]
  )(implicit contextShift:          ContextShift[IO]): IO[SubscriptionsEndpoint[IO]] = IO {
    new SubscriptionsEndpoint[IO](subscriptionCategoryRegistry, logger)
  }
}
