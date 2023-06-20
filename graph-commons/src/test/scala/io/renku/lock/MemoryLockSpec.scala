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

        lock  <- Lock.memory[IO]
        latch <- CountDownLatch[IO](1)

        f1 <- Async[IO].start(latch.await *> lock(Key.global).use(_ => update))
        f2 <- Async[IO].start(latch.await *> lock(Key.global).use(_ => update))
        f3 <- Async[IO].start(latch.await *> lock(Key.global).use(_ => update))

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

        lock  <- Lock.memory[IO]
        latch <- CountDownLatch[IO](1)

        f1 <- Async[IO].start(latch.await *> lock(new Key {}).use(_ => update))
        f2 <- Async[IO].start(latch.await *> lock(new Key {}).use(_ => update))
        f3 <- Async[IO].start(latch.await *> lock(new Key {}).use(_ => update))
        _  <- latch.release
        _  <- List(f1, f2, f3).traverse_(_.join)

        diff <- result.get.map(list => list.max - list.min)
        _ = diff should be < 300.millis
      } yield ()
    }
  }

  "remove mutexes on release" in {
    for {
      (state, lock) <- Lock.memory0[IO]
      latch         <- CountDownLatch[IO](1)

      // initial state is empty
      _ <- state.get.asserting(_ shouldBe empty)

      // one mutex if active
      _ <- Async[IO].start(lock(Key.global).use(_ => latch.await))
      _ <- state.get.asserting(_.size shouldBe 1)

      // once released, mutex must be removed from map
      _ <- latch.release
      _ <- state.get.asserting(_ shouldBe empty)
    } yield ()
  }
}
