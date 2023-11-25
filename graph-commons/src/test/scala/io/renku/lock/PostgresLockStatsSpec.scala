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
import io.renku.lock.PostgresLockStats.Stats
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger

class PostgresLockStatsSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with PostgresLockTest {
  implicit val logger: Logger[IO] = TestLogger[IO]()

  "PostgresLockStats" should {

    "obtain empty statistics" in {
      sessionResource.use { s =>
        for {
          _     <- resetLockTable(s)
          stats <- PostgresLockStats.getStats[IO](s)
          _ = stats shouldBe Stats(0, Nil)
        } yield ()
      }
    }

    "show when a lock is held" in {
      sessionResource.use { s =>
        for {
          _            <- resetLockTable(s)
          (_, release) <- PostgresLock.exclusive_[IO, Int](s).run(1).allocated
          stats        <- PostgresLockStats.getStats(s)
          _            <- release
          _ = stats shouldBe Stats(1, Nil)
        } yield ()
      }
    }

    "insert waiting info" in {
      sessionResource.use { s =>
        for {
          _     <- resetLockTable(s)
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

    "waiting info distinguishes sessions" in {
      (sessionResource, sessionResource).tupled.use { case (s1, s2) =>
        for {
          _     <- resetLockTable(s1)
          _     <- PostgresLockStats.recordWaiting(s1)(5)
          _     <- PostgresLockStats.recordWaiting(s2)(5)
          stats <- PostgresLockStats.getStats(s1)
          _ = stats.waiting.size                  shouldBe 2
          _ = stats.waiting.map(_.pid).toSet.size shouldBe 2
        } yield ()
      }
    }

    "remove waiting info" in {
      sessionResource.use { s =>
        for {
          _     <- resetLockTable(s)
          _     <- PostgresLockStats.recordWaiting(s)(5L)
          _     <- PostgresLockStats.removeWaiting(s)(5L)
          stats <- PostgresLockStats.getStats(s)
          _ = stats shouldBe Stats(0, Nil)
        } yield ()
      }
    }
  }
}
