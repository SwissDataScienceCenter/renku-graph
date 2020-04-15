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

package ch.datascience.dbeventlog.subscriptions

import cats.MonadError
import cats.effect.Effect
import io.chrisdavenport.log4cats.Logger
import org.http4s.dsl.Http4sDsl

import scala.language.higherKinds
import scala.util.control.NonFatal

class SubscriptionsEndpoint[Interpretation[_]: Effect](
    subscriptions: Subscriptions[Interpretation],
    logger:        Logger[Interpretation]
)(implicit ME:     MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import SubscriptionsEndpoint._
  import cats.implicits._
  import ch.datascience.controllers.InfoMessage._
  import ch.datascience.controllers.{ErrorMessage, InfoMessage}
  import org.http4s.circe._
  import org.http4s.{EntityDecoder, Request, Response}

  def addSubscription(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      subscriptionUrl <- request.as[SubscriptionUrl] recoverWith badRequest
      _               <- subscriptions add subscriptionUrl
      response        <- Accepted(InfoMessage("Subscription added"))
    } yield response
  } recoverWith httpResponse

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[SubscriptionUrl]] = {
    case NonFatal(exception) =>
      ME.raiseError(BadRequestError(exception))
  }

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case BadRequestError(exception) => BadRequest(ErrorMessage(exception))
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage("Adding subscription URL failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private implicit lazy val eventEntityDecoder: EntityDecoder[Interpretation, SubscriptionUrl] =
    jsonOf[Interpretation, SubscriptionUrl]
}

object SubscriptionsEndpoint {
  import ch.datascience.tinytypes.json.TinyTypeDecoders.stringDecoder
  import io.circe.Decoder

  case class BadRequestError(cause: Throwable) extends Exception(cause)

  implicit val subscriptionUrlDecoder: Decoder[SubscriptionUrl] =
    _.downField("url").as[SubscriptionUrl](stringDecoder(SubscriptionUrl))
}

object IOSubscriptionsEndpoint {
  import cats.effect.{ContextShift, IO}

  def apply(
      subscriptions:       Subscriptions[IO],
      logger:              Logger[IO]
  )(implicit contextShift: ContextShift[IO]): IO[SubscriptionsEndpoint[IO]] = IO {
    new SubscriptionsEndpoint[IO](subscriptions, logger)
  }
}
