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

package io.renku.eventlog.events.producers
package tsmigrationrequest

import cats.Show
import cats.syntax.all._
import io.circe.Decoder
import io.renku.config.ServiceVersion
import io.renku.events.Subscription
import io.renku.events.Subscription.{SubscriberId, SubscriberUrl}

private final case class MigrationSubscriber(url: SubscriberUrl, id: SubscriberId, version: ServiceVersion)
    extends Subscription.Subscriber

private object MigrationSubscriber {
  implicit lazy val show: Show[MigrationSubscriber] = Show.show { case MigrationSubscriber(url, id, version) =>
    show"subscriber = $url, id = $id, version = $version"
  }

  implicit val decoder: Decoder[MigrationSubscriber] = Decoder.instance { cursor =>
    val subscriberNode = cursor.downField("subscriber")
    for {
      url     <- subscriberNode.downField("url").as[SubscriberUrl]
      id      <- subscriberNode.downField("id").as[SubscriberId]
      version <- subscriberNode.downField("version").as[ServiceVersion]
    } yield MigrationSubscriber(url, id, version)
  }
}
