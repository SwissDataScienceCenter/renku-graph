/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions.unprocessed

import cats.data.NonEmptyList
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, New, NonRecoverableFailure, Processing, RecoverableFailure, Skipped, TriplesStore}
import eu.timepit.refined.auto._
import io.circe.literal._
import io.renku.eventlog.subscriptions.Generators._
import io.renku.eventlog.subscriptions.SubscriberUrl
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

private class SubscriptionRequestDeserializerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "deserialize" should {
    "return the subscriber URL if the statuses are valid" in new TestCase {
      val subscriptionCategoryPayload = subscriptionCategoryPayloads.generateOne
      val payload                     = jsonPayload(acceptedStatuses, subscriptionCategoryPayload.subscriberUrl)
      deserializer.deserialize(payload) shouldBe Success(Some(subscriptionCategoryPayload))
    }

    "return None if the payload does not contain the right supported statuses" in new TestCase {

      val payload = jsonPayload(eventStatuses)
      deserializer.deserialize(payload) shouldBe Success(Option.empty[SubscriptionCategoryPayload])
    }

    "return None if the payload does not contain all the supported statuses" in new TestCase {

      val payload = jsonPayload(singleStatus)
      deserializer.deserialize(payload) shouldBe Success(Option.empty[SubscriptionCategoryPayload])
    }

    "return None if the payload does not contain the right fields" in new TestCase {

      val unsupportedPayload = json"""
          {
          ${nonEmptyStrings().generateOne}: ${nonEmptyStrings().generateOne}
          }
            """

      deserializer.deserialize(unsupportedPayload) shouldBe Success(Option.empty[SubscriptionCategoryPayload])
    }
  }

  trait TestCase {
    val deserializer = SubscriptionRequestDeserializer[Try]()

    val subscriptionCategoryPayloads: Gen[SubscriptionCategoryPayload] = for {
      url <- subscriberUrls
    } yield SubscriptionCategoryPayload(url)

    val acceptedStatuses: Gen[NonEmptyList[EventStatus]] = Gen.const(
      NonEmptyList(
        New,
        List(RecoverableFailure)
      )
    )

    implicit val rejectedStatuses: Gen[NonEmptyList[EventStatus]] = nonEmptyList(
      Gen.oneOf(
        GeneratingTriples,
        TriplesStore,
        Skipped,
        NonRecoverableFailure
      )
    )

    implicit val singleStatus: Gen[NonEmptyList[EventStatus]] = nonEmptyList(
      Gen.oneOf(
        New,
        RecoverableFailure
      ),
      maxElements = 1
    )

    val eventStatuses: Gen[NonEmptyList[EventStatus]] = for {
      accepted <- acceptedStatuses
      rejected <- rejectedStatuses
    } yield accepted ++ rejected.toList

    def jsonPayload(from: Gen[NonEmptyList[EventStatus]], subscriberUrl: SubscriberUrl = subscriberUrls.generateOne) =
      json"""
             {
               "subscriberUrl": ${subscriberUrl.value},
               "statuses": ${from.generateOne.map(_.value)}
             }
          """

  }
}
