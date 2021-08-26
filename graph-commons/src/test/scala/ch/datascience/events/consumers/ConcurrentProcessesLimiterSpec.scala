/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.events.consumers

import cats.effect.{ContextShift, IO}
import cats.effect.concurrent.{Deferred, Semaphore}
import cats.syntax.all._
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{exceptions, positiveInts}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.global

class ConcurrentProcessesLimiterSpec extends AnyWordSpec with MockFactory with should.Matchers with Eventually {

  "tryExecuting" should {

    Set(Accepted, BadRequest).foreach { result =>
      s"execute a process if there are available spots and return the result $result" in new TestCase {
        (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO])
        (() => semaphore.acquire).expects().returning(().pure[IO])
        (() => semaphore.release).expects().returning(().pure[IO])

        var releaseFunctionWasCalled = false
        releaseFunction.expects().onCall(_ => releaseFunctionWasCalled = true)

        limiter.tryExecuting(resultWithCompletion(IO(result)), releaseProcess.some).unsafeRunSync() shouldBe result
        eventually {
          releaseFunctionWasCalled shouldBe true
        }
      }
    }

    "run the releaseProcess and return a SchedulingError if the process fails" in new TestCase {
      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO])
      (() => semaphore.acquire).expects().returning(().pure[IO])
      (() => semaphore.release).expects().returning(().pure[IO])

      var releaseFunctionWasCalled = false
      releaseFunction.expects().onCall(_ => releaseFunctionWasCalled = true)

      val exception = exceptions.generateOne
      val process   = exception.raiseError[IO, EventSchedulingResult]

      limiter
        .tryExecuting(resultWithCompletion(process), releaseProcess.some)
        .unsafeRunSync() shouldBe SchedulingError(exception)

      eventually {
        releaseFunctionWasCalled shouldBe true
      }
    }

    "release the semaphore if the releaseProcess fails" in new TestCase {
      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO])
      (() => semaphore.acquire).expects().returning(().pure[IO])
      (() => semaphore.release).expects().returning(().pure[IO])

      val exception = exceptions.generateOne
      override val releaseProcess: IO[Unit] = exception.raiseError[IO, Unit]

      limiter
        .tryExecuting(resultWithCompletion(IO(Accepted)), releaseProcess.some)
        .unsafeRunSync() shouldBe Accepted
    }

    "release the semaphore once if the releaseProcess and the process fails" in new TestCase {
      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO])
      (() => semaphore.acquire).expects().returning(().pure[IO])
      (() => semaphore.release).expects().returning(().pure[IO])

      val exception = exceptions.generateOne
      val process   = exception.raiseError[IO, EventSchedulingResult]

      val releasingException = exceptions.generateOne
      override val releaseProcess: IO[Unit] = releasingException.raiseError[IO, Unit]

      limiter
        .tryExecuting(resultWithCompletion(process), releaseProcess.some)
        .unsafeRunSync() shouldBe SchedulingError(exception)
    }

    "return Busy if there are no available spots" in new TestCase {
      (() => semaphore.available).expects().returning(0L.pure[IO])

      limiter.tryExecuting(resultWithCompletion(IO(Accepted)), releaseProcess.some).unsafeRunSync() shouldBe Busy
    }

    "throw an error if acquiring the semaphore fails and all spots are available" in new TestCase {
      val exception = exceptions.generateOne
      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO]).twice()
      (() => semaphore.acquire).expects().returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        limiter
          .tryExecuting(resultWithCompletion(IO(Accepted)), releaseProcess.some)
          .unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
    }

    "throw an error if acquiring the semaphore fails and release the semaphore if some spots are not available" in new TestCase {
      val exception = exceptions.generateOne
      inSequence {
        (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO]).once()
        (() => semaphore.acquire).expects().returning(exception.raiseError[IO, Unit])
        (() => semaphore.available).expects().returning(0.toLong.pure[IO]).once()
        (() => semaphore.release).expects().returning(().pure[IO])
      }
      intercept[Exception] {
        limiter
          .tryExecuting(resultWithCompletion(IO(Accepted)), releaseProcess.some)
          .unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
    }

  }

  "withoutLimit" should {
    Set(Accepted, BadRequest).foreach { result =>
      s"execute a process and return the result $result" in new TestCase {
        releaseFunction.expects().returning(())

        withoutLimit
          .tryExecuting(resultWithoutLimit(IO(result)), releaseProcess.some)
          .unsafeRunSync() shouldBe result
      }
    }

    "run the releaseProcess and return a SchedulingError if the process fails" in new TestCase {
      releaseFunction.expects().returning(())

      val exception = exceptions.generateOne
      val process   = exception.raiseError[IO, EventSchedulingResult]

      withoutLimit
        .tryExecuting(resultWithoutLimit(process), releaseProcess.some)
        .unsafeRunSync() shouldBe SchedulingError(exception)
    }

    "return a SchedulingError if both the releaseProcess and the process fails" in new TestCase {

      val exception = exceptions.generateOne
      val process   = exception.raiseError[IO, EventSchedulingResult]

      val releasingException = exceptions.generateOne
      override val releaseProcess: IO[Unit] = releasingException.raiseError[IO, Unit]

      withoutLimit
        .tryExecuting(resultWithoutLimit(process), releaseProcess.some)
        .unsafeRunSync() shouldBe SchedulingError(exception)
    }
  }

  private implicit def cs: ContextShift[IO] = IO.contextShift(global)
  private trait TestCase {

    val processesCount  = positiveInts().generateOne
    val semaphore       = mock[TestSemaphore]
    val releaseFunction = mockFunction[Unit]
    val releaseProcess  = IO(releaseFunction())
    val deferredDone    = Deferred[IO, Unit]

    val limiter      = new ConcurrentProcessesLimiterImpl[IO](processesCount, semaphore) {}
    val withoutLimit = ConcurrentProcessesLimiter.withoutLimit[IO]

    def resultWithoutLimit(result: IO[EventSchedulingResult]): IO[(Deferred[IO, Unit], IO[EventSchedulingResult])] =
      deferredDone.map(_ -> result)

    def resultWithCompletion(result: IO[EventSchedulingResult]): IO[(Deferred[IO, Unit], IO[EventSchedulingResult])] =
      deferredDone.map(done =>
        done -> (for {
          r <- result
          _ <- done.complete(())
        } yield r)
      )
  }

  private trait TestSemaphore extends Semaphore[IO]
}
