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

package io.renku.eventlog.subscriptions.tsmigrationrequest

import cats.MonadThrow
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure, Json}
import io.renku.config.ServiceVersion
import io.renku.eventlog.subscriptions
import io.renku.events.CategoryName
import io.renku.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}

private class SubscriptionPayloadDeserializer[F[_]: MonadThrow]
    extends subscriptions.SubscriptionPayloadDeserializer[F, MigratorSubscriptionInfo] {

  override def deserialize(payload: Json): F[Option[MigratorSubscriptionInfo]] =
    payload.as[MigratorSubscriptionInfo].fold(_ => Option.empty[MigratorSubscriptionInfo], _.some).pure[F]

  private implicit lazy val decoder: Decoder[MigratorSubscriptionInfo] = { cursor =>
    def failIfNot(name: CategoryName): String => Either[DecodingFailure, Unit] = {
      case name.value => ().asRight
      case otherName  => DecodingFailure(s"'$otherName' category event invalid for '$categoryName'", Nil).asLeft
    }

    for {
      _       <- cursor.downField("categoryName").as[String].flatMap(failIfNot(categoryName))
      url     <- cursor.downField("subscriber").downField("url").as[SubscriberUrl]
      id      <- cursor.downField("subscriber").downField("id").as[SubscriberId]
      version <- cursor.downField("subscriber").downField("version").as[ServiceVersion]
    } yield MigratorSubscriptionInfo(url, id, version)
  }
}

private object SubscriptionPayloadDeserializer {
  def apply[F[_]: MonadThrow]: F[SubscriptionPayloadDeserializer[F]] = MonadThrow[F].catchNonFatal {
    new SubscriptionPayloadDeserializer[F]
  }
}
