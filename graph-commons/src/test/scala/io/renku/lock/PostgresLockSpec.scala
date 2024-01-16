/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import fs2.{Pipe, concurrent}
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.db.TestDB
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Warn
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger
import skunk._
import skunk.data._
import skunk.exception.EofException
import skunk.implicits._
import skunk.net.protocol.{Describe, Parse}
import skunk.util.Typer

import scala.concurrent.duration._

class PostgresLockSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with PostgresLockTest {

  "PostgresLock.exclusive" should {
    val pollInterval = 100.millis

    "sequentially on same key" in testDBResource.use { cfg =>
      implicit val logger: Logger[IO] = TestLogger()

      val lock = exclusiveLock(cfg, pollInterval)

      for {
        result <- Ref.of[IO, List[FiniteDuration]](Nil)
        update = IO.sleep(200.millis) *> IO.realTime.flatMap(time => result.update(time :: _))
        latch <- CountDownLatch[IO](1)

        f1 <- Async[IO].start(latch.await *> lock("p1").use(_ => update))
        f2 <- Async[IO].start(latch.await *> lock("p1").use(_ => update))
        f3 <- Async[IO].start(latch.await *> lock("p1").use(_ => update))

        _ <- latch.release
        _ <- List(f1, f2, f3).traverse_(_.join)

        diff <- result.get.map(list => list.max - list.min)
        _ = diff should be >= 400.millis
      } yield ()
    }

    "sequentially on same key using session constructor" in testDBResource.use { cfg =>
      implicit val logger: Logger[IO] = TestLogger()

      def createLock = sessionResource(cfg).map(makeExclusiveLock(_, pollInterval))

      (createLock, createLock, createLock).tupled.use { case (l1, l2, l3) =>
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

    "parallel on different key" in testDBResource.use { cfg =>
      implicit val logger: Logger[IO] = TestLogger()

      val lock = exclusiveLock(cfg)

      for {
        result <- Ref.of[IO, List[FiniteDuration]](Nil)
        update = IO.sleep(200.millis) *> IO.realTime.flatMap(time => result.update(time :: _))
        latch <- CountDownLatch[IO](1)

        f1 <- Async[IO].start(latch.await *> lock("p1").use(_ => update))
        f2 <- Async[IO].start(latch.await *> lock("p2").use(_ => update))
        f3 <- Async[IO].start(latch.await *> lock("p3").use(_ => update))

        _ <- latch.release
        _ <- List(f1, f2, f3).traverse_(_.join)

        diff <- result.get.map(list => list.max - list.min)
        _ = diff should be < 300.millis
      } yield ()
    }

    "log and retry if acquiring fails" in testDBResource.use { cfg =>
      implicit val logger: TestLogger[IO] = TestLogger()

      val exception   = new Exception("boom")
      val makeSession = createFailingSessionThatRecovers(cfg, exception)

      val interval = 50.millis
      def lock     = makeSession.map(PostgresLock.exclusive_[IO, String](_, interval))

      lock.flatMap(_("p1")).use(_ => IO.unit).asserting { _ =>
        logger.logged(
          Warn(s"Acquiring postgres advisory lock failed! Retry in $interval.", exception)
        )
      }
    }

    "recreate the lock with a new session once the connection has been closed" in testDBResource.use { cfg =>
      implicit val logger: TestLogger[IO] = TestLogger()

      val exception              = EofException(bytesRequested = 1, bytesRead = 0)
      val failingSessionResource = createFailingSessionThatDoesNotRecover(cfg, exception)

      val interval = 50.millis

      val lock = PostgresLock.exclusive[IO, String](failingSessionResource, interval)

      for {
        _   <- sessionResource(cfg).use(resetLockTable)
        res <- lock.run("p1").use(_ => IO.unit).assertNoException
        _ <- logger.loggedF(
               Warn("Session get closed while acquiring postgres advisory lock! Renewing connection.", exception)
             )
      } yield res
    }
  }

  "PostgresLock.shared" should {
    "allow multiple shared locks" in testDBResource.use { cfg =>
      implicit val logger: Logger[IO] = TestLogger()
      val lock = sharedLock(cfg)

      for {
        result <- Ref.of[IO, List[FiniteDuration]](Nil)
        update = IO.sleep(200.millis) *> IO.realTime.flatMap(time => result.update(time :: _))
        latch <- CountDownLatch[IO](1)

        f1 <- Async[IO].start(latch.await *> lock("p1").use(_ => update))
        f2 <- Async[IO].start(latch.await *> lock("p1").use(_ => update))
        f3 <- Async[IO].start(latch.await *> lock("p1").use(_ => update))

        _ <- latch.release
        _ <- List(f1, f2, f3).traverse_(_.join)

        diff <- result.get.map(list => list.max - list.min)
        _ = diff should be < 300.millis
      } yield ()
    }
  }

  "PostgresLock stats" should {
    "log if writing/removing stats records fail" in testDBResource.use { cfg =>
      implicit val logger: TestLogger[IO] = TestLogger()
      sessionResource(cfg).use { s =>
        for {
          _            <- s.execute(sql"DROP TABLE IF EXISTS kg_lock_stats".command)
          (_, release) <- makeExclusiveLock(s).run("1").allocated
          _            <- release

          key = LongKey[String].asLong("1")
          _ = logger.getMessages(TestLogger.Level.Error).map(_.message) shouldBe
                List(s"Failed to remove lock stats record for key=$key")
        } yield ()
      }
    }

    "show when a session is waiting for a lock" in testDBResource.use { cfg =>
      implicit val logger: Logger[IO] = TestLogger()
      (sessionResource(cfg), sessionResource(cfg)).tupled.use { case (s1, s2) =>
        for {
          _            <- resetLockTable(s1)
          (_, release) <- makeExclusiveLock(s1, 1.second).run("1").allocated
          fiber        <- Async[IO].start(makeExclusiveLock(s2).run("1").allocated)
          _            <- IO.sleep(100.millis)
          stats        <- PostgresLockStats.getStats(cfg.name, s1)
          _            <- release
          _            <- fiber.join
          stats2       <- PostgresLockStats.getStats(cfg.name, s1)

          _ = stats.waiting.size shouldBe 1
          _ = stats2.waiting     shouldBe Nil
        } yield ()
      }
    }

    "remove records for the owning session only" in testDBResource.use { cfg =>
      implicit val logger: Logger[IO] = TestLogger()

      (sessionResource(cfg), sessionResource(cfg), sessionResource(cfg)).tupled.use { case (s1, s2, s3) =>
        for {
          _            <- resetLockTable(s1)
          (_, release) <- makeExclusiveLock(s1).run("1").allocated
          f1           <- Async[IO].start(makeExclusiveLock(s2, 4.millis).run("1").allocated)
          // use a longer interval so that there is no attempt to insert another record
          f2 <- Async[IO].start(makeExclusiveLock(s3, 1.second).run("1").allocated)
          _  <- IO.sleep(100.millis)

          // there must be two records waiting for the same lock
          stats <- PostgresLockStats.getStats(cfg.name, s1)
          _ = stats.currentLocks                       shouldBe 1
          _ = stats.waiting.size                       shouldBe 2
          _ = stats.waiting.map(_.objectId).toSet.size shouldBe 1
          _ = stats.waiting.map(_.pid).toSet.size      shouldBe 2

          // releasing the lock so that f1 or f2 grabs it
          // then it must not remove the record from the other one
          _ <- release
          _ <- IO.sleep(200.millis)

          stats2 <- PostgresLockStats.getStats(cfg.name, s1)
          _ = stats2.waiting.size shouldBe 1
          _ = stats2.currentLocks shouldBe 1

          _ <- List(f1, f2).parTraverse_(_.join.flatMap {
                 case Outcome.Succeeded(rel) => rel.flatMap(_._2)
                 case _                      => IO.raiseError(new Exception("joining failed"))
               })
        } yield ()
      }
    }
  }

  private def createFailingSessionThatRecovers(cfg: DBConfig[TestDB], exception: Exception): Resource[IO, Session[IO]] =
    (Resource.eval(Ref.of[IO, Int](0)), sessionResource(cfg))
      .mapN { (counter, goodSession) =>
        new PostgresLockSpec.TestSession {
          override def unique[A, B](query: Query[A, B])(args: A) =
            counter.getAndUpdate(_ + 1) >>= {
              case n if n < 2 => IO.raiseError(exception)
              case _          => goodSession.unique(query)(args)
            }

          override def execute[A](command: Command[A])(args: A) =
            goodSession.execute(command)(args)
        }
      }

  private def createFailingSessionThatDoesNotRecover(cfg:       DBConfig[TestDB],
                                                     exception: Exception
  ): Resource[IO, Session[IO]] = {
    val counter = Ref.unsafe[IO, Int](0)

    sessionResource(cfg).map { goodSession =>
      new PostgresLockSpec.TestSession {
        override def unique[A, B](query: Query[A, B])(args: A) =
          counter.getAndUpdate(_ + 1) >>= {
            case n if n < 2 => IO.raiseError(exception)
            case _          => goodSession.unique(query)(args)
          }

        override def execute[A](command: Command[A])(args: A) =
          goodSession.execute(command)(args)
      }
    }
  }
}

object PostgresLockSpec {
  abstract class TestSession extends Session[IO] {
    override def execute[A, B](query:  Query[A, B])(args:      A): IO[List[B]] = ???
    override def option[A, B](query:   Query[A, B])(args:      A): IO[Option[B]] = ???
    override def stream[A, B](command: Query[A, B])(args:      A, chunkSize: Int): fs2.Stream[IO, B] = ???
    override def cursor[A, B](query:   Query[A, B])(args:      A): Resource[IO, Cursor[IO, B]] = ???
    override def execute[A](command:   Command[A])(args:       A): IO[Completion] = ???
    override def pipe[A](command:      Command[A]): Pipe[IO, A, Completion] = ???
    override def pipe[A, B](query:     Query[A, B], chunkSize: Int): Pipe[IO, A, B] = ???
    override def parameters: concurrent.Signal[IO, Map[String, String]] = ???
    override def parameter(key: String): fs2.Stream[IO, String] = ???
    override def transactionStatus: concurrent.Signal[IO, TransactionStatus] = ???
    override def execute[A](query:    Query[skunk.Void, A]): IO[List[A]] = ???
    override def unique[A](query:     Query[skunk.Void, A]): IO[A] = ???
    override def unique[A, B](query:  Query[A, B])(args: A): IO[B] = ???
    override def option[A](query:     Query[skunk.Void, A]): IO[Option[A]] = ???
    override def execute(command:     Command[skunk.Void]): IO[Completion] = ???
    override def prepare[A, B](query: Query[A, B]): IO[PreparedQuery[IO, A, B]] = ???
    override def prepare[A](command:  Command[A]): IO[PreparedCommand[IO, A]] = ???
    override def channel(name:        Identifier): Channel[IO, String, String] = ???
    override def transaction[A]: Resource[IO, Transaction[IO]] = ???
    override def transaction[A](
        isolationLevel: TransactionIsolationLevel,
        accessMode:     TransactionAccessMode
    ): Resource[IO, Transaction[IO]] = ???
    override def typer:         Typer              = ???
    override def describeCache: Describe.Cache[IO] = ???
    override def parseCache:    Parse.Cache[IO]    = ???
  }
}
