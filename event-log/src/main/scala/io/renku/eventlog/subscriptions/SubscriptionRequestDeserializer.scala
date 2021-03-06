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
import ch.datascience.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import ch.datascience.graph.model.events.CategoryName
import io.circe.{Decoder, Json}
import io.renku.eventlog.subscriptions.SubscriptionRequestDeserializer.PayloadFactory

private trait SubscriptionRequestDeserializer[Interpretation[_], SubscriptionInfoType <: SubscriptionInfo] {
  def deserialize(payload: Json): Interpretation[Option[SubscriptionInfoType]]
}

private object SubscriptionRequestDeserializer {

  type PayloadFactory[SubscriptionInfoType] = (SubscriberUrl, SubscriberId, Option[Capacity]) => SubscriptionInfoType

  def apply[Interpretation[_], SubscriptionInfoType <: SubscriptionInfo](
      categoryName:   CategoryName,
      payloadFactory: PayloadFactory[SubscriptionInfoType]
  )(implicit
      monadError: MonadError[Interpretation, Throwable]
  ): Interpretation[SubscriptionRequestDeserializer[Interpretation, SubscriptionInfoType]] = monadError.catchNonFatal {
    new SubscriptionRequestDeserializerImpl(categoryName, payloadFactory)
  }
}

private class SubscriptionRequestDeserializerImpl[Interpretation[_], SubscriptionInfoType <: SubscriptionInfo](
    categoryName:      CategoryName,
    payloadFactory:    PayloadFactory[SubscriptionInfoType]
)(implicit monadError: MonadError[Interpretation, Throwable])
    extends SubscriptionRequestDeserializer[Interpretation, SubscriptionInfoType] {

  import cats.syntax.all._

  override def deserialize(payload: Json): Interpretation[Option[SubscriptionInfoType]] =
    payload
      .as[(String, SubscriberUrl, SubscriberId, Option[Capacity])]
      .fold(_ => Option.empty[SubscriptionInfoType], toCategoryPayload)
      .pure[Interpretation]

  private lazy val toCategoryPayload
      : ((String, SubscriberUrl, SubscriberId, Option[Capacity])) => Option[SubscriptionInfoType] = {
    case (categoryName.value, subscriberUrl, subscriberId, maybeCapacity) =>
      payloadFactory(subscriberUrl, subscriberId, maybeCapacity).some
    case _ => None
  }

  private implicit lazy val payloadDecoder: Decoder[(String, SubscriberUrl, SubscriberId, Option[Capacity])] = {
    cursor =>
      for {
        categoryName  <- cursor.downField("categoryName").as[String]
        subscriberUrl <- cursor.downField("subscriber").downField("url").as[SubscriberUrl]
        subscriberId  <- cursor.downField("subscriber").downField("id").as[SubscriberId]
        maybeCapacity <- cursor.downField("subscriber").downField("capacity").as[Option[Capacity]]
      } yield (categoryName, subscriberUrl, subscriberId, maybeCapacity)
  }
}
