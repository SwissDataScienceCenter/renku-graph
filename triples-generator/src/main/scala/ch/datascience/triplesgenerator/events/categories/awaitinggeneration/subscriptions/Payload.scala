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

import ch.datascience.graph.model.events.CategoryName
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.GenerationProcessesNumber
import ch.datascience.triplesgenerator.events.subscriptions.SubscriberUrl

private[events] final case class Payload(categoryName:  CategoryName,
                                         subscriberUrl: SubscriberUrl,
                                         capacity:      GenerationProcessesNumber
) extends subscriptions.SubscriptionPayload

private[events] object Payload {
  import io.circe.Encoder
  import io.circe.literal._

  val encoder: Encoder[Payload] = Encoder.instance[Payload] { payload =>
    json"""{
        "categoryName":  ${payload.categoryName.value},
        "subscriberUrl": ${payload.subscriberUrl.value},
        "capacity":      ${payload.capacity.value}
      }"""
  }
}
