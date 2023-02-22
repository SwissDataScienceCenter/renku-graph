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

import cats.Show
import cats.syntax.all._
import io.circe.{Decoder, Encoder}
import io.circe.literal._
import io.renku.events.Generators._
import io.renku.events.Subscription
import io.renku.events.Subscription._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.positiveInts
import org.scalacheck.Gen

object Generators {

  val totalCapacities: Gen[TotalCapacity] = positiveInts() map (v => TotalCapacity(v.value))
  val freeCapacities:  Gen[FreeCapacity]  = positiveInts() map (v => FreeCapacity(v.value))
  val usedCapacities:  Gen[UsedCapacity]  = positiveInts() map (v => UsedCapacity(v.value))

  private[producers] final case class TestSubscriber(url: SubscriberUrl, id: SubscriberId, capacity: SubscriberCapacity)
      extends Subscription.Subscriber
      with Subscription.DefinedCapacity

  private[producers] object TestSubscriber {

    implicit val encoder: Encoder[TestSubscriber] = Encoder.instance { case TestSubscriber(url, id, capacity) =>
      json"""{
        "subscriber": {
          "url":      $url,
          "id":       $id,
          "capacity": $capacity
        }
      }"""
    }

    implicit val decoder: Decoder[TestSubscriber] = Decoder.instance { cursor =>
      val subscriberNode = cursor.downField("subscriber")
      for {
        url      <- subscriberNode.downField("url").as[SubscriberUrl]
        id       <- subscriberNode.downField("id").as[SubscriberId]
        capacity <- subscriberNode.downField("capacity").as[SubscriberCapacity]
      } yield TestSubscriber(url, id, capacity)
    }

    implicit val show: Show[TestSubscriber] = Show.show { info =>
      show"subscriber = ${info.url}, id = ${info.id}, capacity = ${info.capacity}"
    }
  }

  private[producers] implicit val testSubscribers: Gen[TestSubscriber] = for {
    url      <- subscriberUrls
    id       <- subscriberIds
    capacity <- subscriberCapacities
  } yield TestSubscriber(url, id, capacity)

  private[producers] implicit val sendingResults: Gen[EventsSender.SendingResult] =
    Gen.oneOf(EventsSender.SendingResult.all)

  implicit val eventProducerStatuses: Gen[EventProducerStatus] = for {
    categoryName <- categoryNames
    capacity     <- (totalCapacities -> freeCapacities).mapN(EventProducerStatus.Capacity).toGeneratorOfOptions
  } yield EventProducerStatus(categoryName, capacity)
}
