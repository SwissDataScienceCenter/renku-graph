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

package ch.datascience.events.consumers.subscriptions

import cats.Show
import cats.syntax.all._
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.microservices.{MicroserviceBaseUrl, MicroserviceIdentifier}
import ch.datascience.tinytypes.constraints.{NonBlank, Url}
import ch.datascience.tinytypes.json.TinyTypeDecoders.stringDecoder
import ch.datascience.tinytypes.{StringTinyType, TinyTypeConverter, TinyTypeFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.circe.Decoder

import java.net.URL

trait SubscriptionPayload extends Product with Serializable {
  val categoryName: CategoryName
  val subscriber:   Subscriber
}

final case class CategoryAndUrlPayload(categoryName: CategoryName, subscriber: Subscriber) extends SubscriptionPayload

object CategoryAndUrlPayload {
  import io.circe.Encoder
  import io.circe.literal._

  implicit val encoder: Encoder[CategoryAndUrlPayload] = Encoder.instance[CategoryAndUrlPayload] { payload =>
    json"""{
        "categoryName":  ${payload.categoryName.value},
        "subscriber": {
          "url": ${payload.subscriber.url.value},
          "id": ${payload.subscriber.id.value}
        }
      }"""
  }
}

trait Subscriber extends Product with Serializable {
  val url: SubscriberUrl
  val id:  SubscriberId
}

final case class SubscriberBasicInfo(url: SubscriberUrl, id: SubscriberId) extends Subscriber

final class SubscriberUrl private (val value: String) extends AnyVal with StringTinyType
object SubscriberUrl extends TinyTypeFactory[SubscriberUrl](new SubscriberUrl(_)) with Url {

  def apply(microserviceBaseUrl: MicroserviceBaseUrl, part: String Refined NonEmpty): SubscriberUrl =
    SubscriberUrl((microserviceBaseUrl / part.toString()).toString)

  implicit val decoder: Decoder[SubscriberUrl] = stringDecoder(SubscriberUrl)

  implicit val microserviceBaseUrlConverter: TinyTypeConverter[SubscriberUrl, MicroserviceBaseUrl] =
    (subscriberUrl: SubscriberUrl) => {
      val url = new URL(subscriberUrl.value)
      MicroserviceBaseUrl(s"${url.getProtocol}://${url.getHost}:${url.getPort}").asRight[IllegalArgumentException]
    }

  implicit lazy val show: Show[SubscriberUrl] = Show.show(url => show"url = ${url.value}")
}

final class SubscriberId private (val value: String) extends AnyVal with StringTinyType
object SubscriberId extends TinyTypeFactory[SubscriberId](new SubscriberId(_)) with NonBlank {

  def apply(microserviceId: MicroserviceIdentifier): SubscriberId = SubscriberId(microserviceId.toString)

  implicit val decoder: Decoder[SubscriberId] = stringDecoder(SubscriberId)

  implicit lazy val show: Show[SubscriberId] = Show.show(id => show"id = ${id.value}")
}
