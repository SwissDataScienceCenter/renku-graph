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

package io.renku.eventlog.subscriptions

import cats.effect.IO
import io.circe.Json

trait SubscriptionCategoryRegistry[Interpretation[_]] {

  def run(): Interpretation[Unit]

  def register(subscriptionRequest: Json): Interpretation[Either[RequestError, Unit]]
}

private[subscriptions] class SubscriptionCategoryRegistryImpl[Interpretation[_]](
    categories: Set[SubscriptionCategory[Interpretation, SubscriptionCategoryPayload]]
) extends SubscriptionCategoryRegistry[Interpretation] {
  override def run(): Interpretation[Unit] = ??? // inst

  override def register(subscriptionRequest: Json): Interpretation[Either[RequestError, Unit]] = ???
}

object IOSubscriptionCategoryRegistry {
  def apply(): IO[SubscriptionCategoryRegistry[IO]] = {
    val categories = ???
    IO(new SubscriptionCategoryRegistryImpl[IO](categories))
  }
}
