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
import cats.implicits.showInterpolator
import io.circe.Decoder
import io.renku.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import io.renku.graph.model.projects
import io.renku.tinytypes._
import io.renku.tinytypes.constraints.{NonNegativeInt, Url}
import io.renku.tinytypes.json.TinyTypeDecoders.{intDecoder, stringDecoder}

private final case class ProjectIds(id: projects.Id, path: projects.Path)

private final class Capacity private (val value: Int) extends AnyVal with IntTinyType
private object Capacity extends TinyTypeFactory[Capacity](new Capacity(_)) with NonNegativeInt {
  implicit val decoder: Decoder[Capacity] = intDecoder(Capacity)
}

private trait SubscriptionInfo extends Product with Serializable {
  val subscriberUrl: SubscriberUrl
  val subscriberId:  SubscriberId
  val maybeCapacity: Option[Capacity]
}

private trait UrlAndIdSubscriptionInfo extends SubscriptionInfo {
  override val subscriberUrl: SubscriberUrl
  override val subscriberId:  SubscriberId
  override val maybeCapacity: Option[Capacity]

  override def equals(obj: Any): Boolean = obj match {
    case info: SubscriptionInfo => info.subscriberUrl == subscriberUrl
    case _ => false
  }

  override def hashCode(): Int = subscriberUrl.hashCode()
}

private object UrlAndIdSubscriptionInfo {

  implicit def show[T <: UrlAndIdSubscriptionInfo]: Show[T] =
    Show.show(info => show"subscriber = ${info.subscriberUrl}, id = ${info.subscriberId}${info.maybeCapacity}")

  private implicit lazy val showCapacity: Show[Option[Capacity]] =
    Show.show(maybeCapacity => maybeCapacity.map(capacity => show" with capacity ${capacity.value}").getOrElse(""))
}

private final class SourceUrl private (val value: String) extends AnyVal with StringTinyType
private object SourceUrl extends TinyTypeFactory[SourceUrl](new SourceUrl(_)) with Url {
  implicit val decoder: Decoder[SourceUrl] = stringDecoder(SourceUrl)
}
