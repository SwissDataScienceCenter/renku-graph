/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.events

import cats.Show
import cats.syntax.all._
import io.circe.{Decoder, Encoder}
import io.circe.literal._
import DefaultSubscription.DefaultSubscriber
import Subscription._

final case class DefaultSubscription(categoryName: CategoryName, subscriber: DefaultSubscriber) extends Subscription

object DefaultSubscription {

  implicit val encoder: Encoder[DefaultSubscription] = Encoder.instance[DefaultSubscription] {
    case DefaultSubscription(categoryName, subscriber) => json"""{
      "categoryName": $categoryName,
      "subscriber":   $subscriber
    }"""
  }

  implicit val decoder: Decoder[DefaultSubscription] = Decoder.instance { cursor =>
    for {
      categoryName <- cursor.downField("categoryName").as[CategoryName]
      subscriber   <- cursor.as[DefaultSubscriber]
    } yield DefaultSubscription(categoryName, subscriber)
  }

  sealed trait DefaultSubscriber extends Subscriber {
    def fold[A](wocf: DefaultSubscriber.WithoutCapacity => A, wcf: DefaultSubscriber.WithCapacity => A): A
  }

  object DefaultSubscriber {

    def apply(url: SubscriberUrl, id: SubscriberId): DefaultSubscriber.WithoutCapacity =
      WithoutCapacity(url, id)

    def apply(url: SubscriberUrl, id: SubscriberId, capacity: SubscriberCapacity): DefaultSubscriber.WithCapacity =
      WithCapacity(url, id, capacity)

    final case class WithoutCapacity(url: SubscriberUrl, id: SubscriberId) extends DefaultSubscriber {
      override def fold[A](wocf: WithoutCapacity => A, wcf: WithCapacity => A): A = wocf(this)
    }

    final case class WithCapacity(url: SubscriberUrl, id: SubscriberId, capacity: SubscriberCapacity)
        extends DefaultSubscriber
        with DefinedCapacity {
      override def fold[A](wocf: WithoutCapacity => A, wcf: WithCapacity => A): A = wcf(this)
    }

    implicit val encoder: Encoder[DefaultSubscriber] = Encoder.instance {
      case WithoutCapacity(url, id) => json"""{
        "url": $url,
        "id":  $id
      }"""
      case WithCapacity(url, id, capacity) => json"""{
        "url":      $url,
        "id":       $id,
        "capacity": $capacity
      }"""
    }

    implicit val decoder: Decoder[DefaultSubscriber] = Decoder.instance { cursor =>
      val subscriberNode = cursor.downField("subscriber")
      for {
        url           <- subscriberNode.downField("url").as[SubscriberUrl]
        id            <- subscriberNode.downField("id").as[SubscriberId]
        maybeCapacity <- subscriberNode.downField("capacity").as[Option[SubscriberCapacity]]
      } yield maybeCapacity match {
        case Some(capacity) => DefaultSubscriber(url, id, capacity)
        case None           => DefaultSubscriber(url, id)
      }
    }

    implicit def show[T <: DefaultSubscriber]: Show[T] = Show.show {
      case DefaultSubscriber.WithoutCapacity(url, id)   => show"subscriber = $url, id = $id"
      case DefaultSubscriber.WithCapacity(url, id, cap) => show"subscriber = $url, id = $id, capacity = $cap"
    }
  }
}
