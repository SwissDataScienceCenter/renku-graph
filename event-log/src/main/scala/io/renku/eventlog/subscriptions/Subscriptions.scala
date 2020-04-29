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
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.log4cats.Logger

import scala.language.higherKinds

class Subscriptions[Interpretation[_]] private[subscriptions] (
    currentUrl: Ref[Interpretation, Option[SubscriberUrl]],
    logger:     Logger[Interpretation]
)(implicit ME:  MonadError[Interpretation, Throwable]) {
  import scala.collection.JavaConverters._

  private val subscriptionsPool = new ConcurrentHashMap[SubscriberUrl, Unit]()

  def add(subscriberUrl: SubscriberUrl): Interpretation[Unit] = ME.catchNonFatal {
    val present = subscriptionsPool.containsKey(subscriberUrl)
    subscriptionsPool.putIfAbsent(subscriberUrl, ())
    if (!present) logger.info(s"$subscriberUrl added")
    ()
  }

  def next: Interpretation[Option[SubscriberUrl]] = {

    def replaceCurrentUrl(maybeUrl: Option[SubscriberUrl]) = currentUrl.set(maybeUrl) map (_ => maybeUrl)

    getAll.flatMap {
      case Nil => currentUrl.getAndSet(Option.empty[SubscriberUrl])
      case urls =>
        currentUrl.get flatMap {
          case None => currentUrl.set(urls.headOption) map (_ => urls.headOption)
          case Some(url) =>
            urls.indexOf(url) match {
              case -1                          => replaceCurrentUrl(urls.headOption)
              case idx if idx < urls.size - 1  => replaceCurrentUrl(Some(urls(idx + 1)))
              case idx if idx == urls.size - 1 => replaceCurrentUrl(urls.headOption)
            }
        }
    }
  }

  def isNext: Interpretation[Boolean] = (!subscriptionsPool.isEmpty).pure[Interpretation]

  def hasOtherThan(url: SubscriberUrl): Interpretation[Boolean] = getAll map (_.exists(_ != url))

  def getAll: Interpretation[List[SubscriberUrl]] = ME.catchNonFatal {
    subscriptionsPool.keys().asScala.toList
  }

  def remove(subscriberUrl: SubscriberUrl): Interpretation[Unit] = ME.catchNonFatal {
    val present = subscriptionsPool.containsKey(subscriberUrl)
    subscriptionsPool remove subscriberUrl
    if (present) logger.info(s"$subscriberUrl removed")
    ()
  }
}

object Subscriptions {
  import cats.effect.IO

  def apply(logger: Logger[IO]): IO[Subscriptions[IO]] =
    for {
      currentUrl <- Ref.of[IO, Option[SubscriberUrl]](None)
    } yield new Subscriptions[IO](currentUrl, logger)
}
