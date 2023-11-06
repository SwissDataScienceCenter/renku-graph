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

package io.renku.events

import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.circe.Decoder
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceIdentifier}
import io.renku.tinytypes._
import io.renku.tinytypes.constraints.{NonBlank, PositiveInt, Url}
import Subscription._
import io.renku.tinytypes.json.TinyTypeDecoders.{intDecoder, stringDecoder}

import java.net.URI

trait Subscription {
  val categoryName: CategoryName
  val subscriber:   Subscriber
}

object Subscription {

  trait Subscriber extends Product {
    val url: SubscriberUrl
    val id:  SubscriberId

    override def equals(obj: Any): Boolean = obj match {
      case info: Subscriber => info.url == url
      case _ => false
    }

    override def hashCode(): Int = url.hashCode()
  }

  trait DefinedCapacity {
    self: Subscriber =>
    val capacity: SubscriberCapacity
  }

  final class SubscriberUrl private (val value: String) extends AnyVal with StringTinyType
  object SubscriberUrl extends TinyTypeFactory[SubscriberUrl](new SubscriberUrl(_)) with Url[SubscriberUrl] {

    def apply(microserviceBaseUrl: MicroserviceBaseUrl, part: String Refined NonEmpty): SubscriberUrl =
      SubscriberUrl((microserviceBaseUrl / part.toString()).toString)

    implicit val decoder: Decoder[SubscriberUrl] = stringDecoder(SubscriberUrl)

    implicit val microserviceBaseUrlConverter: TinyTypeConverter[SubscriberUrl, MicroserviceBaseUrl] =
      (subscriberUrl: SubscriberUrl) => {
        val url = new URI(subscriberUrl.value).toURL
        MicroserviceBaseUrl(s"${url.getProtocol}://${url.getHost}:${url.getPort}").asRight[IllegalArgumentException]
      }
  }

  final class SubscriberId private (val value: String) extends AnyVal with StringTinyType
  object SubscriberId extends TinyTypeFactory[SubscriberId](new SubscriberId(_)) with NonBlank[SubscriberId] {

    def apply(microserviceId: MicroserviceIdentifier): SubscriberId = SubscriberId(microserviceId.toString)

    implicit val decoder: Decoder[SubscriberId] = stringDecoder(SubscriberId)
  }

  final class SubscriberCapacity private (val value: Int) extends AnyVal with IntTinyType
  object SubscriberCapacity
      extends TinyTypeFactory[SubscriberCapacity](new SubscriberCapacity(_))
      with PositiveInt[SubscriberCapacity] {
    implicit val decoder: Decoder[SubscriberCapacity] = intDecoder(SubscriberCapacity)
  }
}
