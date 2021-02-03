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

import ch.datascience.graph.model.projects
import ch.datascience.tinytypes.constraints.{InstantNotInTheFuture, NonNegativeInt, Url}
import ch.datascience.tinytypes.json.TinyTypeDecoders.{intDecoder, stringDecoder}
import ch.datascience.tinytypes._
import io.circe.Decoder

import java.time.Instant

private final case class ProjectIds(id: projects.Id, path: projects.Path)

private final class SubscriberUrl private (val value: String) extends AnyVal with StringTinyType
private object SubscriberUrl extends TinyTypeFactory[SubscriberUrl](new SubscriberUrl(_)) with Url {
  implicit val decoder: Decoder[SubscriberUrl] = stringDecoder(SubscriberUrl)
}

private final class Capacity private (val value: Int) extends AnyVal with IntTinyType
private object Capacity extends TinyTypeFactory[Capacity](new Capacity(_)) with NonNegativeInt {
  implicit val decoder: Decoder[Capacity] = intDecoder(Capacity)
}

final class LastSyncedDate private (val value: Instant) extends AnyVal with InstantTinyType
object LastSyncedDate extends TinyTypeFactory[LastSyncedDate](new LastSyncedDate(_)) with InstantNotInTheFuture

private trait SubscriptionInfo extends Product with Serializable {
  val subscriberUrl: SubscriberUrl
  val maybeCapacity: Option[Capacity]

  override def equals(obj: Any): Boolean = obj match {
    case info: SubscriptionInfo => info.subscriberUrl == subscriberUrl
    case _ => false
  }

  override def hashCode(): Int = subscriberUrl.hashCode()

  override lazy val toString = {
    val capacityAsString = maybeCapacity.map(capacity => s" with capacity $capacity").getOrElse("")
    s"$subscriberUrl$capacityAsString"
  }
}
