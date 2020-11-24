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

package io.renku.eventlog.subscriptions.unprocessed

import cats.MonadError
import cats.implicits.catsSyntaxApplicativeId
import cats.syntax.all._
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus.{New, RecoverableFailure}
import io.circe
import io.circe.Decoder
import io.renku.eventlog.subscriptions
import io.renku.eventlog.subscriptions.SubscriberUrl
import io.renku.eventlog.subscriptions.unprocessed.SubscriptionRequestDeserializer.UrlAndStatuses

// TODO: make this private[unprocessed]
private[eventlog] case class SubscriptionRequestDeserializer[Interpretation[_]]()(implicit
    monadError: MonadError[Interpretation, Throwable]
) extends subscriptions.SubscriptionRequestDeserializer[Interpretation, SubscriberUrl] {
  override def deserialize(payload: circe.Json): Interpretation[Option[SubscriberUrl]] =
    payload
      .as[UrlAndStatuses]
      .fold(_ => Option.empty[SubscriberUrl], maybeSubscriptionUrl)
      .pure[Interpretation]

  private val acceptedStatuses = Set(New, RecoverableFailure)
  private def maybeSubscriptionUrl(urlAndStatuses: UrlAndStatuses): Option[SubscriberUrl] =
    if (urlAndStatuses.eventStatuses != acceptedStatuses) Option.empty[SubscriberUrl]
    else urlAndStatuses.subscriberUrl.some
}

private[eventlog] object SubscriptionRequestDeserializer {

  case class UrlAndStatuses(subscriberUrl: SubscriberUrl, eventStatuses: Set[EventStatus])

  private implicit val payloadDecoder: Decoder[UrlAndStatuses] = { cursor =>
    for {
      subscriberUrl <- cursor.downField("subscriberUrl").as[SubscriberUrl]
      statuses      <- cursor.downField("statuses").as[List[EventStatus]]
    } yield UrlAndStatuses(subscriberUrl, statuses.toSet)
  }
}
