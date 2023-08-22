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

package io.renku.triplesstore.client.http

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration._

class RetrySpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers {

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  case object RetryError extends RuntimeException("retry error") {
    def unapply(ex: Throwable) =
      ex match {
        case RetryError => Some(this)
        case _          => None
      }
  }
  case object FinalError extends RuntimeException("final error")

  final class TestEffect(f: Int => Option[Throwable]) {
    val execTimes: Ref[IO, List[FiniteDuration]] = Ref.unsafe(Nil)

    def exec: IO[Unit] =
      Clock[IO].monotonic.flatMap { time =>
        execTimes.flatModify { list =>
          f(list.size) match {
            case None    => (time :: list, IO.unit)
            case Some(r) => (time :: list, IO.raiseError(r))
          }
        }
      }

    def assertPauseTime(expect: FiniteDuration, epsilon: Duration = 50.millis): IO[Assertion] =
      execTimes.get.map { times =>
        val lowerBound = expect - epsilon
        val upperBound = expect + epsilon

        val diffs = times
          .zip(times.tail)
          .map { case (a, b) => a - b }

        all(diffs) should (be > lowerBound and be < upperBound)
      }
  }

  it should "not retry and not wait on first success" in {
    val e     = new TestEffect(_ => None)
    val retry = new Retry[IO](Retry.RetryConfig(5.hours, 10))
    for {
      r <- retry.retryWhen(RetryError.unapply(_).isDefined)(e.exec)
      _ = r shouldBe ()
      execCount <- e.execTimes.get
      _ = execCount.size shouldBe 1
    } yield ()
  }

  it should "not retry for unfiltered errors" in {
    val e     = new TestEffect(_ => FinalError.some)
    val retry = Retry[IO](Retry.RetryConfig(5.hours, 10))
    for {
      r <- retry.retryWhen(RetryError.unapply(_).isDefined)(e.exec).attempt
      _ = r shouldBe Left(FinalError)
      execCount <- e.execTimes.get
      _ = execCount.size shouldBe 1
    } yield ()
  }

  it should "retry in the given interval until success result" in {
    val e = new TestEffect(
      Map(
        0 -> RetryError.some,
        1 -> RetryError.some
      ).withDefaultValue(None)
    )
    val retry = Retry[IO](Retry.RetryConfig(500.millis, 10))
    for {
      r <- retry.retryWhen(RetryError.unapply(_).isDefined)(e.exec)
      _ = r shouldBe ()
      execCount <- e.execTimes.get
      _ = execCount.size shouldBe 3
      _ <- e.assertPauseTime(500.millis, epsilon = 100.millis)
    } yield ()
  }

  it should "retry in the given interval  until final error" in {
    val e = new TestEffect(
      Map(
        0 -> RetryError.some,
        1 -> RetryError.some,
        2 -> FinalError.some
      ).withDefaultValue(None)
    )
    val retry = Retry[IO](Retry.RetryConfig(500.millis, 10))
    for {
      r <- retry.retryWhen(RetryError.unapply(_).isDefined)(e.exec).attempt
      _ = r shouldBe Left(FinalError)
      execCount <- e.execTimes.get
      _ = execCount.size shouldBe 3
      _ <- e.assertPauseTime(500.millis)
    } yield ()
  }

  it should "retry and give up after max tries" in {
    val e = new TestEffect(
      Map(
        0 -> RetryError.some,
        1 -> RetryError.some,
        2 -> RetryError.some,
        3 -> RetryError.some
      ).withDefaultValue(None)
    )
    val retry = Retry[IO](Retry.RetryConfig(1.millis, 3))
    for {
      r <- retry.retryWhen(RetryError.unapply(_).isDefined)(e.exec).attempt
      Left(ex: Retry.RetryExceeded) = r
      _                             = ex.getCause    shouldBe RetryError
      _                             = ex.errors.size shouldBe 3
      execCount <- e.execTimes.get
      _ = execCount.size shouldBe 3
    } yield ()
  }
}
