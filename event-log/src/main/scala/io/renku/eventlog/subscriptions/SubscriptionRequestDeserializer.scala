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
import io.circe.{Decoder, Json}
import io.renku.eventlog.subscriptions.SubscriptionRequestDeserializer.PayloadFactory
import io.renku.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import io.renku.graph.model.events.CategoryName

private trait SubscriptionRequestDeserializer[F[_], SubscriptionInfoType <: SubscriptionInfo] {
  def deserialize(payload: Json): F[Option[SubscriptionInfoType]]
}

private object SubscriptionRequestDeserializer {

  type PayloadFactory[SubscriptionInfoType] = (SubscriberUrl, SubscriberId, Option[Capacity]) => SubscriptionInfoType

  def apply[F[_]: MonadThrow, SubscriptionInfoType <: SubscriptionInfo](
      categoryName:   CategoryName,
      payloadFactory: PayloadFactory[SubscriptionInfoType]
  ): F[SubscriptionRequestDeserializer[F, SubscriptionInfoType]] =
    MonadThrow[F].catchNonFatal {
      new SubscriptionRequestDeserializerImpl(categoryName, payloadFactory)
    }
}

private class SubscriptionRequestDeserializerImpl[F[_]: MonadThrow, SubscriptionInfoType <: SubscriptionInfo](
    categoryName:   CategoryName,
    payloadFactory: PayloadFactory[SubscriptionInfoType]
) extends SubscriptionRequestDeserializer[F, SubscriptionInfoType] {

  import cats.syntax.all._

  override def deserialize(payload: Json): F[Option[SubscriptionInfoType]] =
    payload
      .as[(String, SubscriberUrl, SubscriberId, Option[Capacity])]
      .fold(_ => Option.empty[SubscriptionInfoType], toCategoryPayload)
      .pure[F]

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
