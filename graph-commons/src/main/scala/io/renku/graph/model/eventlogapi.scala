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

package io.renku.graph.model

import cats.syntax.all._
import io.circe.Decoder
import io.renku.events.CategoryName

object eventlogapi {

  import ServiceStatus._
  final case class ServiceStatus(subscriptions: Set[SubscriptionStatus])

  object ServiceStatus {

    final case class SubscriptionStatus(name: CategoryName, maybeCapacity: Option[Capacity])

    object SubscriptionStatus {
      implicit val jsonDecoder: Decoder[SubscriptionStatus] = Decoder.instance { cur =>
        (cur.downField("categoryName").as[CategoryName] -> cur.downField("capacity").as[Option[Capacity]])
          .mapN(SubscriptionStatus(_, _))
      }
    }

    final case class Capacity(total: Int, free: Int)

    object Capacity {
      implicit val jsonDecoder: Decoder[Capacity] = Decoder.instance { cur =>
        (cur.downField("total").as[Int] -> cur.downField("free").as[Int])
          .mapN(Capacity(_, _))
      }
    }

    implicit val jsonDecoder: Decoder[ServiceStatus] =
      Decoder
        .instance(_.downField("subscriptions").as[List[SubscriptionStatus]])
        .map(subscriptions => ServiceStatus(subscriptions.toSet))
  }
}
