package io.renku.lock

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class PostgresLockStatsSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with PostgresLockTest {

  "PostgresLockStats" should {
    "obtain empty statistics" in withContainers { cnt =>
      session(cnt).use(PostgresLockStats.getStats[IO]).asserting(_ shouldBe Nil)
    }

    "show when a lock is held" in withContainers { cnt =>
      IO.unit
    }

    "show when a session is waiting for a lock" in withContainers { cnt =>
      IO.unit
    }
  }
}
