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
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.interpreters.TestLogger
import io.renku.lock.PostgresLockStats.{Stats, Waiting}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import java.time.{Duration, OffsetDateTime}

class PostgresLockStatsSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with PostgresLockTest {
  implicit val logger: Logger[IO] = TestLogger[IO]()

  "PostgresLockStats" should {
    "obtain empty statistics" in withContainers { cnt =>
      session(cnt).use { s =>
        for {
          _     <- PostgresLockStats.ensureStatsTable(s)
          stats <- PostgresLockStats.getStats[IO](s)
          _ = stats shouldBe Stats(0, Nil)
        } yield ()
      }
    }

    "show when a lock is held" in withContainers { cnt =>
      session(cnt).use { s =>
        for {
          _            <- PostgresLockStats.ensureStatsTable(s)
          (_, release) <- PostgresLock.exclusive[IO, Int](s).run(1).allocated
          stats        <- PostgresLockStats.getStats(s)
          _            <- release
          _ = stats shouldBe Stats(1, Nil)
        } yield ()
      }
    }

    "insert waiting info" in withContainers { cnt =>
      session(cnt).use { s =>
        for {
          _     <- PostgresLockStats.ensureStatsTable(s)
          _     <- PostgresLockStats.recordWaiting(s)(5L)
          stats <- PostgresLockStats.getStats(s)
          _ = stats.currentLocks shouldBe 0
          _ = stats.waiting.size shouldBe 1

          _      <- PostgresLockStats.recordWaiting(s)(5L)
          stats2 <- PostgresLockStats.getStats(s)
          _ = stats shouldBe stats2.copy(waiting =
                stats2.waiting.map(_.copy(waitDuration = stats.waiting.head.waitDuration))
              )
        } yield ()
      }
    }

    "remove waiting info" in withContainers { cnt =>
      session(cnt).use { s =>
        for {
          _     <- PostgresLockStats.ensureStatsTable(s)
          _     <- PostgresLockStats.recordWaiting(s)(5L)
          _     <- PostgresLockStats.removeWaiting(s)(5L)
          stats <- PostgresLockStats.getStats(s)
          _ = stats shouldBe Stats(0, Nil)
        } yield ()
      }
    }

    "show when a session is waiting for a lock" in withContainers { cnt =>
      (session(cnt), session(cnt)).tupled.use { case (s1, _) =>
        for { // ???
          _            <- IO.print("***** TEST START")
          _            <- PostgresLockStats.ensureStatsTable(s1)
          (_, release) <- PostgresLock.exclusive[IO, Int](s1).run(1).allocated
          // fiber        <- Async[IO].start(PostgresLock.exclusive[IO, Int](s2).run(1).allocated)
          _     <- IO.sleep(10.millis)
          _     <- IO.println("GET STATS")
          stats <- PostgresLockStats.getStats(s1)
          _     <- release
          // _            <- fiber.join
          _ <- IO.print("***** TEST END")
          _ = stats shouldBe Stats(1, List(Waiting(1, 2, OffsetDateTime.now(), Duration.ZERO)))
        } yield ()
      }
    }
  }
}
