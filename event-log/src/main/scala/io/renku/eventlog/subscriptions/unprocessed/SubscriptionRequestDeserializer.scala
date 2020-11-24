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
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.tinytypes.json.TinyTypeDecoders.stringDecoder
import io.circe.{Decoder, Json}
import io.renku.eventlog.subscriptions.unprocessed.UnprocessedSubscriptionRequestDeserializer.UrlAndStatuses
import io.renku.eventlog.subscriptions.{SubscriberUrl, SubscriptionRequest, SubscriptionRequestDeserializer}

// TODO: make this private[unprocessed]
private[eventlog] case class UnprocessedSubscriptionRequestDeserializer[Interpretation[_]]()(implicit
    monadError: MonadError[Interpretation, Throwable]
) extends SubscriptionRequestDeserializer[Interpretation, UrlAndStatuses] {
  override def deserialize(payload: Json): Interpretation[UrlAndStatuses] =
    payload
      .as[UrlAndStatuses]
      .fold(monadError.raiseError[UrlAndStatuses](_), urlAndStatuses => urlAndStatuses.pure[Interpretation])

  implicit val payloadDecoder: Decoder[UrlAndStatuses] = cursor =>
    for {
      subscriberUrl <- cursor.downField("subscriberUrl").as[SubscriberUrl](stringDecoder(SubscriberUrl))
      statuses      <- cursor.downField("statuses").as[List[EventStatus]]
    } yield UrlAndStatuses(subscriberUrl, statuses.toSet)
}

object UnprocessedSubscriptionRequestDeserializer {

  case class UrlAndStatuses(subscriberUrl: SubscriberUrl, eventStatuses: Set[EventStatus]) extends SubscriptionRequest

}
