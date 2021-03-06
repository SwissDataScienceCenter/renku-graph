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

package ch.datascience.triplesgenerator.events
package categories.awaitinggeneration.subscriptions

import ch.datascience.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.GenerationProcessesNumber

private final case class Payload(categoryName: CategoryName, subscriber: Subscriber)
    extends ch.datascience.events.consumers.subscriptions.SubscriptionPayload

private object Payload {
  import io.circe.Encoder
  import io.circe.literal._

  implicit val encoder: Encoder[Payload] = Encoder.instance[Payload] { payload =>
    json"""{
        "categoryName": ${payload.categoryName.value},
        "subscriber": {
          "url":      ${payload.subscriber.url.value},
          "id":       ${payload.subscriber.id.value},
          "capacity": ${payload.subscriber.capacity.value}
        }
      }"""
  }
}

private final case class Subscriber(
    url:      SubscriberUrl,
    id:       SubscriberId,
    capacity: GenerationProcessesNumber
) extends ch.datascience.events.consumers.subscriptions.Subscriber
