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

package io.renku.eventsqueue

import cats.Applicative
import cats.data.Kleisli
import cats.effect.{Deferred, IO, Ref, Temporal}
import cats.syntax.all._
import org.scalatest.Suite
import skunk.data.Identifier
import skunk.{Channel, Session}

import scala.concurrent.duration._

trait EventsQueueDBSpec extends ContainerDB { self: Suite =>

  private type IOChannel = Channel[IO, String, String]

  private val timeout = 20 seconds

  def listenOnChannelUntil(channel: Identifier, condition: List[String] => Boolean): IO[Deferred[IO, IO[Unit]]] =
    Deferred.apply[IO, IO[Unit]].flatTap { conditionMet =>
      IO.race(
        waitForNotifications(channel, condition, conditionMet),
        failWithTimeout(conditionMet)
      ).start
    }

  private def failWithTimeout(conditionMet: Deferred[IO, IO[Unit]]) =
    Temporal[IO].delayBy(
      conditionMet.complete(
        new Exception(s"Condition on received events not met after ${timeout.toSeconds} sec").raiseError[IO, Unit]
      ),
      timeout
    )

  private def waitForNotifications(channel:      Identifier,
                                   condition:    List[String] => Boolean,
                                   conditionMet: Deferred[IO, IO[Unit]]
  ) = withChannel(channel) { ch =>
    val accu = Ref.unsafe[IO, List[String]](List.empty)
    ch.listen(20)
      .evalMap(n => accu.updateAndGet(old => (n.value :: old.reverse).reverse))
      .map(condition)
      .takeThrough(!_)
      .evalMap(Applicative[IO].whenA(_)(conditionMet.complete(().pure[IO])))
      .compile
      .drain
  }

  private def withChannel(channel: Identifier)(f: IOChannel => IO[Unit]): IO[Unit] =
    execute[Unit] {
      Kleisli.fromFunction[IO, Session[IO]](_.channel(channel)).flatMapF(f)
    }
}
