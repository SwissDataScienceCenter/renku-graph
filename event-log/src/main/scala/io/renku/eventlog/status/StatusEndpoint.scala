/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.status

import cats.MonadThrow
import cats.syntax.all._
import io.renku.eventlog.events.producers.EventProducerStatus.Capacity
import io.renku.eventlog.events.producers.{EventProducerStatus, EventProducersRegistry}
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait StatusEndpoint[F[_]] {
  def `GET /status`: F[Response[F]]
}

object StatusEndpoint {
  def apply[F[_]: MonadThrow: Logger](eventProducersRegistry: EventProducersRegistry[F]): F[StatusEndpoint[F]] =
    new StatusEndpointImpl(eventProducersRegistry).pure[F].widen
}

private class StatusEndpointImpl[F[_]: MonadThrow: Logger](eventProducersRegistry: EventProducersRegistry[F])
    extends Http4sDsl[F]
    with StatusEndpoint[F] {

  import eu.timepit.refined.auto._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import io.renku.data.Message
  import org.http4s.circe.CirceEntityEncoder._

  override def `GET /status`: F[Response[F]] =
    eventProducersRegistry.getStatus
      .flatMap(toResponse)
      .recoverWith(httpResult)

  private def toResponse(statuses: Set[EventProducerStatus]): F[Response[F]] = Ok {
    json"""{
      "subscriptions": ${statuses.toList}
    }"""
  }

  private lazy val httpResult: PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val message = Message.Error("Finding EL status failed")
    Logger[F].error(exception)(message.show) >> InternalServerError(message.asJson)
  }

  private implicit lazy val producerStatusDecoder: Encoder[EventProducerStatus] = Encoder.instance {
    case EventProducerStatus(categoryName, None) =>
      json"""{"categoryName": $categoryName}"""
    case EventProducerStatus(categoryName, Some(capacity)) =>
      json"""{"categoryName": $categoryName, "capacity": $capacity}"""
  }

  private implicit lazy val capacityDecoder: Encoder[Capacity] = Encoder.instance { case Capacity(total, free) =>
    json"""{"total":  $total, "free": $free}"""
  }
}
