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

import java.util.concurrent.ConcurrentHashMap

import cats.MonadError
import io.chrisdavenport.log4cats.Logger

import scala.language.higherKinds

class Subscriptions[Interpretation[_]](
    logger:    Logger[Interpretation]
)(implicit ME: MonadError[Interpretation, Throwable]) {
  import scala.collection.JavaConverters._

  private val subscriptionsPool = new ConcurrentHashMap[SubscriptionUrl, Unit]()

  def add(subscriptionUrl: SubscriptionUrl): Interpretation[Unit] = ME.catchNonFatal {
    val present = subscriptionsPool.containsKey(subscriptionUrl)
    subscriptionsPool.putIfAbsent(subscriptionUrl, ())
    if (!present) logger.info(s"$subscriptionUrl added")
    ()
  }

  def getAll: Interpretation[List[SubscriptionUrl]] = ME.catchNonFatal {
    subscriptionsPool.keys().asScala.toList
  }

  def remove(subscriptionUrl: SubscriptionUrl): Interpretation[Unit] = ME.catchNonFatal {
    val present = subscriptionsPool.containsKey(subscriptionUrl)
    subscriptionsPool remove subscriptionUrl
    if (present) logger.info(s"$subscriptionUrl removed")
    ()
  }
}

object IOSubscriptions {
  import cats.effect.IO

  def apply(logger: Logger[IO]): IO[Subscriptions[IO]] = IO {
    new Subscriptions[IO](logger)
  }
}
