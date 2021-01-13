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
import ch.datascience.graph.model.events.CategoryName
import io.circe.{Decoder, Json}

private trait SubscriptionRequestDeserializer[Interpretation[_], PayloadType <: SubscriptionCategoryPayload] {
  def deserialize(payload: Json): Interpretation[Option[PayloadType]]
}

private object SubscriptionRequestDeserializer {

  def apply[Interpretation[_], PayloadType <: SubscriptionCategoryPayload](
      categoryName:   CategoryName,
      payloadFactory: SubscriberUrl => PayloadType
  )(implicit
      monadError: MonadError[Interpretation, Throwable]
  ): Interpretation[SubscriptionRequestDeserializer[Interpretation, PayloadType]] = monadError.catchNonFatal {
    new SubscriptionRequestDeserializerImpl(categoryName, payloadFactory)
  }
}

private class SubscriptionRequestDeserializerImpl[Interpretation[_], PayloadType <: SubscriptionCategoryPayload](
    categoryName:      CategoryName,
    payloadFactory:    SubscriberUrl => PayloadType
)(implicit monadError: MonadError[Interpretation, Throwable])
    extends SubscriptionRequestDeserializer[Interpretation, PayloadType] {

  import cats.syntax.all._

  override def deserialize(payload: Json): Interpretation[Option[PayloadType]] =
    payload
      .as[(String, SubscriberUrl)]
      .fold(_ => Option.empty[PayloadType], toCategoryPayload)
      .pure[Interpretation]

  private lazy val toCategoryPayload: ((String, SubscriberUrl)) => Option[PayloadType] = {
    case (categoryName.value, subscriberUrl) => Some(payloadFactory(subscriberUrl))
    case _                                   => None
  }

  private implicit lazy val payloadDecoder: Decoder[(String, SubscriberUrl)] = { cursor =>
    for {
      categoryName  <- cursor.downField("categoryName").as[String]
      subscriberUrl <- cursor.downField("subscriberUrl").as[SubscriberUrl]
    } yield categoryName -> subscriberUrl
  }
}
