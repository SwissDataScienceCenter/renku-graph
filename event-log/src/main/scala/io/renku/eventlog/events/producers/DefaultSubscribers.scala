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

import cats.effect.Async
import cats.syntax.all._
import io.renku.events.CategoryName
import io.renku.events.DefaultSubscription.DefaultSubscriber
import org.typelevel.log4cats.Logger

private object DefaultSubscribers {

  type DefaultSubscribers[F[_]] = Subscribers[F, DefaultSubscriber]

  def apply[F[_]](implicit ev: DefaultSubscribers[F]): DefaultSubscribers[F] = ev

  def apply[F[_]: Async: Logger: DefaultSubscriberTracker](categoryName: CategoryName): F[DefaultSubscribers[F]] =
    SubscribersRegistry[F](categoryName)
      .map(new SubscribersImpl[F, DefaultSubscriber](categoryName, _))
}
