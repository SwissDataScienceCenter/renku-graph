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
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import io.renku.db.PostgresContainer
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import skunk.Session
import natchez.Trace.Implicits.noop

import scala.concurrent.duration._

class PostgresLockSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with TestContainerForAll {

  override val containerDef = PostgreSQLContainer.Def(
    dockerImageName = PostgresContainer.imageName,
    databaseName = "locktest",
    username = "pg",
    password = "pg"
  )

  def session(c: Containers): Resource[IO, Session[IO]] =
    Session.single[IO](
      host = c.host,
      port = c.underlyingUnsafeContainer.getFirstMappedPort,
      user = c.username,
      database = c.databaseName,
      password = Some(c.password)
    )

  "PostgresLock.exclusiveBlocking" should {

    "sequentially on same key" in withContainers { cnt =>
      def makeLock = session(cnt).map(PostgresLock.exclusiveBlocking[IO, String])

      (makeLock, makeLock, makeLock).tupled.use { case (l1, l2, l3) =>
        for {
          result <- Ref.of[IO, List[FiniteDuration]](Nil)
          update = IO.sleep(200.millis) *> IO.realTime.flatMap(time => result.update(time :: _))
          latch <- CountDownLatch[IO](1)

          f1 <- Async[IO].start(latch.await *> l1("p1").use(_ => update))
          f2 <- Async[IO].start(latch.await *> l2("p1").use(_ => update))
          f3 <- Async[IO].start(latch.await *> l3("p1").use(_ => update))

          _ <- latch.release
          _ <- List(f1, f2, f3).traverse_(_.join)

          diff <- result.get.map(list => list.max - list.min)
          _ = diff should be >= 400.millis
        } yield ()
      }
    }

    "parallel on different key" in withContainers { cnt =>
      def makeLock = session(cnt).map(PostgresLock.exclusiveBlocking[IO, String])

      (makeLock, makeLock, makeLock).tupled.use { case (l1, l2, l3) =>
        for {
          result <- Ref.of[IO, List[FiniteDuration]](Nil)
          update = IO.sleep(200.millis) *> IO.realTime.flatMap(time => result.update(time :: _))
          latch <- CountDownLatch[IO](1)

          f1 <- Async[IO].start(latch.await *> l1("p1").use(_ => update))
          f2 <- Async[IO].start(latch.await *> l2("p2").use(_ => update))
          f3 <- Async[IO].start(latch.await *> l3("p3").use(_ => update))

          _ <- latch.release
          _ <- List(f1, f2, f3).traverse_(_.join)

          diff <- result.get.map(list => list.max - list.min)
          _ = diff should be < 300.millis
        } yield ()
      }
    }
  }

  "PostgresLock.exclusivePolling" should {
    val pollInterval = 100.millis

    "sequentially on same key" in withContainers { cnt =>
      def makeLock = session(cnt).map(PostgresLock.exclusive[IO, String](_, pollInterval))

      (makeLock, makeLock, makeLock).tupled.use { case (l1, l2, l3) =>
        for {
          result <- Ref.of[IO, List[FiniteDuration]](Nil)
          update = IO.sleep(200.millis) *> IO.realTime.flatMap(time => result.update(time :: _))
          latch <- CountDownLatch[IO](1)

          f1 <- Async[IO].start(latch.await *> l1("p1").use(_ => update))
          f2 <- Async[IO].start(latch.await *> l2("p1").use(_ => update))
          f3 <- Async[IO].start(latch.await *> l3("p1").use(_ => update))

          _ <- latch.release
          _ <- List(f1, f2, f3).traverse_(_.join)

          diff <- result.get.map(list => list.max - list.min)
          _ = diff should be >= 400.millis
        } yield ()
      }
    }

    "parallel on different key" in withContainers { cnt =>
      def makeLock = session(cnt).map(PostgresLock.exclusive[IO, String](_))

      (makeLock, makeLock, makeLock).tupled.use { case (l1, l2, l3) =>
        for {
          result <- Ref.of[IO, List[FiniteDuration]](Nil)
          update = IO.sleep(200.millis) *> IO.realTime.flatMap(time => result.update(time :: _))
          latch <- CountDownLatch[IO](1)

          f1 <- Async[IO].start(latch.await *> l1("p1").use(_ => update))
          f2 <- Async[IO].start(latch.await *> l2("p2").use(_ => update))
          f3 <- Async[IO].start(latch.await *> l3("p3").use(_ => update))

          _ <- latch.release
          _ <- List(f1, f2, f3).traverse_(_.join)

          diff <- result.get.map(list => list.max - list.min)
          _ = diff should be < 300.millis
        } yield ()
      }
    }
  }

  "PostgresLock.shared" should {
    "allow multiple shared locks" in withContainers { cnt =>
      def makeLock = session(cnt).map(PostgresLock.shared[IO, String](_))

      (makeLock, makeLock, makeLock).tupled.use { case (l1, l2, l3) =>
        for {
          result <- Ref.of[IO, List[FiniteDuration]](Nil)
          update = IO.sleep(200.millis) *> IO.realTime.flatMap(time => result.update(time :: _))
          latch <- CountDownLatch[IO](1)

          f1 <- Async[IO].start(latch.await *> l1("p1").use(_ => update))
          f2 <- Async[IO].start(latch.await *> l2("p1").use(_ => update))
          f3 <- Async[IO].start(latch.await *> l3("p1").use(_ => update))

          _ <- latch.release
          _ <- List(f1, f2, f3).traverse_(_.join)

          diff <- result.get.map(list => list.max - list.min)
          _ = diff should be < 300.millis
        } yield ()
      }
    }
  }
  "PostgresLock.sharedBlocking" should {
    "allow multiple shared locks" in withContainers { cnt =>
      def makeLock = session(cnt).map(PostgresLock.sharedBlocking[IO, String])

      (makeLock, makeLock, makeLock).tupled.use { case (l1, l2, l3) =>
        for {
          result <- Ref.of[IO, List[FiniteDuration]](Nil)
          update = IO.sleep(200.millis) *> IO.realTime.flatMap(time => result.update(time :: _))
          latch <- CountDownLatch[IO](1)

          f1 <- Async[IO].start(latch.await *> l1("p1").use(_ => update))
          f2 <- Async[IO].start(latch.await *> l2("p1").use(_ => update))
          f3 <- Async[IO].start(latch.await *> l3("p1").use(_ => update))

          _ <- latch.release
          _ <- List(f1, f2, f3).traverse_(_.join)

          diff <- result.get.map(list => list.max - list.min)
          _ = diff should be < 300.millis
        } yield ()
      }
    }
  }
}
