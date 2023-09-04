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

package io.renku.lock

import cats.effect._
import cats.effect.std.CountDownLatch
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._

class MemoryLockSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers {

  def debug(m: String): IO[Unit] =
    IO.realTimeInstant.map(t => s"$t : $m").flatMap(IO.println)

  "memory lock" should {

    "sequentially on same key" in {
      for {
        result <- Ref.of[IO, List[FiniteDuration]](Nil)
        update = IO.sleep(200.millis) *> IO.realTime.flatMap(time => result.update(time :: _))

        lock  <- Lock.memory[IO, Int]
        latch <- CountDownLatch[IO](1)

        f1 <- Async[IO].start(latch.await *> lock(1).use(_ => update))
        f2 <- Async[IO].start(latch.await *> lock(1).use(_ => update))
        f3 <- Async[IO].start(latch.await *> lock(1).use(_ => update))

        _ <- latch.release
        _ <- List(f1, f2, f3).traverse_(_.join)

        diff <- result.get.map(list => list.max - list.min)
        _ = diff should be >= 400.millis
      } yield ()
    }

    "parallel on different key" in {
      for {
        result <- Ref.of[IO, List[FiniteDuration]](Nil)
        update = IO.sleep(200.millis) *> IO.realTime.flatMap(time => result.update(time :: _))

        lock  <- Lock.memory[IO, Int]
        latch <- CountDownLatch[IO](1)

        f1 <- Async[IO].start(latch.await *> lock(1).use(_ => update))
        f2 <- Async[IO].start(latch.await *> lock(2).use(_ => update))
        f3 <- Async[IO].start(latch.await *> lock(3).use(_ => update))
        _  <- latch.release
        _  <- List(f1, f2, f3).traverse_(_.join)

        diff <- result.get.map(list => list.max - list.min)
        _ = diff should be < 300.millis
      } yield ()
    }
  }

  "remove mutexes on release" in {
    for {
      (state, lock)  <- Lock.memory0[IO, Int]
      finalised      <- Deferred.apply[IO, Unit]
      startReleasing <- Deferred.apply[IO, Unit]
      lockAcquired   <- Deferred[IO, Unit]

      // initial state is empty
      _ <- state.get.asserting(_ shouldBe empty)

      // one mutex if active
      _ <- Async[IO].start(
             lock(1).onFinalize(finalised.complete(()).void).use(_ => lockAcquired.complete(()) >> startReleasing.get)
           )
      _ <- lockAcquired.get
      _ <- state.get.asserting(_.size shouldBe 1)

      // once released, mutex must be removed from map
      _ <- startReleasing.complete(())
      _ <- finalised.get
      _ <- state.get.asserting(_ shouldBe empty)
    } yield ()
  }
}
