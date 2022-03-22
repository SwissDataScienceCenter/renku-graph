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

package io.renku.eventlog.subscriptions

import cats.Show
import cats.syntax.all._
import io.renku.events.consumers.subscriptions._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.positiveInts
import org.scalacheck.Gen

private object Generators {

  val capacities: Gen[Capacity] = positiveInts() map (v => Capacity(v.value))

  final case class TestUrlAndIdSubscriptionInfo(subscriberUrl: SubscriberUrl,
                                                subscriberId:  SubscriberId,
                                                maybeCapacity: Option[Capacity]
  ) extends UrlAndIdSubscriptionInfo

  final case class TestSubscriptionInfo(subscriberUrl: SubscriberUrl,
                                        subscriberId:  SubscriberId,
                                        maybeCapacity: Option[Capacity]
  ) extends SubscriptionInfo

  object TestSubscriptionInfo {
    implicit val show: Show[TestSubscriptionInfo] = Show.show { info =>
      show"subscriber = ${info.subscriberUrl}, id = ${info.subscriberId}${info.maybeCapacity}"
    }
  }

  implicit val urlAndIdSubscriptionInfos: Gen[TestUrlAndIdSubscriptionInfo] = for {
    url           <- subscriberUrls
    id            <- subscriberIds
    maybeCapacity <- capacities.toGeneratorOfOptions
  } yield TestUrlAndIdSubscriptionInfo(url, id, maybeCapacity)

  implicit val subscriptionInfos: Gen[TestSubscriptionInfo] = for {
    url           <- subscriberUrls
    id            <- subscriberIds
    maybeCapacity <- capacities.toGeneratorOfOptions
  } yield TestSubscriptionInfo(url, id, maybeCapacity)
}
