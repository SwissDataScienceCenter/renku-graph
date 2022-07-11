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

package io.renku.eventlog.events.producers

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.events.CategoryName
import org.typelevel.log4cats.Logger

private object UrlAndIdSubscribers {

  type UrlAndIdSubscribers[F[_]] = Subscribers[F, UrlAndIdSubscriptionInfo]

  def apply[F[_]](implicit subscribers: UrlAndIdSubscribers[F]): UrlAndIdSubscribers[F] = subscribers

  def apply[F[_]: Async: UrlAndIdSubscriberTracker: Logger](
      categoryName: CategoryName
  ): F[UrlAndIdSubscribers[F]] = for {
    subscribersRegistry <- SubscribersRegistry[F](categoryName)
    subscribers         <- MonadThrow[F].catchNonFatal(new SubscribersImpl(categoryName, subscribersRegistry))
  } yield subscribers
}
