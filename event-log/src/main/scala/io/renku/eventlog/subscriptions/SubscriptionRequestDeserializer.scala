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
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.graph.model.events.CategoryName
import io.circe.{Decoder, Json}

private trait SubscriptionRequestDeserializer[Interpretation[_], SubscriptionInfoType <: SubscriptionInfo] {
  def deserialize(payload: Json): Interpretation[Option[SubscriptionInfoType]]
}

private object SubscriptionRequestDeserializer {

  def apply[Interpretation[_], SubscriptionInfoType <: SubscriptionInfo](
      categoryName:   CategoryName,
      payloadFactory: (SubscriberUrl, Option[Capacity]) => SubscriptionInfoType
  )(implicit
      monadError: MonadError[Interpretation, Throwable]
  ): Interpretation[SubscriptionRequestDeserializer[Interpretation, SubscriptionInfoType]] = monadError.catchNonFatal {
    new SubscriptionRequestDeserializerImpl(categoryName, payloadFactory)
  }
}

private class SubscriptionRequestDeserializerImpl[Interpretation[_], SubscriptionInfoType <: SubscriptionInfo](
    categoryName:      CategoryName,
    payloadFactory:    (SubscriberUrl, Option[Capacity]) => SubscriptionInfoType
)(implicit monadError: MonadError[Interpretation, Throwable])
    extends SubscriptionRequestDeserializer[Interpretation, SubscriptionInfoType] {

  import cats.syntax.all._

  override def deserialize(payload: Json): Interpretation[Option[SubscriptionInfoType]] =
    payload
      .as[(String, SubscriberUrl, Option[Capacity])]
      .fold(_ => Option.empty[SubscriptionInfoType], toCategoryPayload)
      .pure[Interpretation]

  private lazy val toCategoryPayload: ((String, SubscriberUrl, Option[Capacity])) => Option[SubscriptionInfoType] = {
    case (categoryName.value, subscriberUrl, maybeCapacity) => Some(payloadFactory(subscriberUrl, maybeCapacity))
    case _                                                  => None
  }

  private implicit lazy val payloadDecoder: Decoder[(String, SubscriberUrl, Option[Capacity])] = { cursor =>
    for {
      categoryName  <- cursor.downField("categoryName").as[String]
      subscriberUrl <- cursor.downField("subscriberUrl").as[SubscriberUrl]
      maybeCapacity <- cursor.downField("capacity").as[Option[Capacity]]
    } yield (categoryName, subscriberUrl, maybeCapacity)
  }
}
