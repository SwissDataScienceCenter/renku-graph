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

import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{httpUrls, nonBlankStrings, positiveInts}
import ch.datascience.graph.model.events.CategoryName
import org.scalacheck.Gen

private object Generators {

  val subscriberUrls: Gen[SubscriberUrl] = httpUrls() map SubscriberUrl.apply
  val capacities:     Gen[Capacity]      = positiveInts() map (v => Capacity(v.value))
  val categoryNames:  Gen[CategoryName]  = nonBlankStrings() map (value => CategoryName(value.value))

  final case class TestSubscriptionInfo(subscriberUrl: SubscriberUrl, maybeCapacity: Option[Capacity])
      extends SubscriptionInfo

  implicit val subscriptionInfos: Gen[TestSubscriptionInfo] = for {
    url           <- subscriberUrls
    maybeCapacity <- capacities.toGeneratorOfOptions
  } yield TestSubscriptionInfo(url, maybeCapacity)
}
