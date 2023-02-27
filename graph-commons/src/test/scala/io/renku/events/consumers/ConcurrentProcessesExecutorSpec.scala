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

package io.renku.events.consumers

import cats.data.EitherT
import cats.effect.IO
import cats.effect.kernel.Deferred
import cats.effect.std.Semaphore
import cats.syntax.all._
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.generators.Generators.{exceptions, positiveInts}
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicBoolean

class ConcurrentProcessesExecutorSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  "tryExecuting" should {

    s"execute a process if there are available spots and return the result" in new TestCase {
      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO])
      (() => semaphore.acquire).expects().returning(().pure[IO])
      (() => semaphore.release).expects().returning(().pure[IO])

      val releaseFunctionWasCalled = new AtomicBoolean(false)

      tryExecuting(
        (deferred: Deferred[IO, Unit]) =>
          EitherT
            .rightT[IO, EventSchedulingResult](Accepted)
            .semiflatTap(_ => deferred.complete(())),
        IO.delay(releaseFunctionWasCalled.set(true))
      ).unsafeRunSync() shouldBe Accepted

      eventually {
        releaseFunctionWasCalled.get() shouldBe true
      }
    }

    s"execute a process if there are available spots and return BadRequest" in new TestCase {
      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO])
      (() => semaphore.acquire).expects().returning(().pure[IO])
      (() => semaphore.release).expects().returning(().pure[IO])

      val releaseFunctionWasCalled = new AtomicBoolean(false)

      tryExecuting(
        _ => EitherT.leftT[IO, Accepted](BadRequest),
        IO.delay(releaseFunctionWasCalled.set(true))
      ).unsafeRunSync() shouldBe BadRequest

      eventually {
        releaseFunctionWasCalled.get() shouldBe false
      }
    }

    "run the releaseProcess and return a SchedulingError if the process fails" in new TestCase {
      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO])
      (() => semaphore.acquire).expects().returning(().pure[IO])
      (() => semaphore.release).expects().returning(().pure[IO])

      val releaseFunctionWasCalled = new AtomicBoolean(false)

      val exception = exceptions.generateOne

      tryExecuting(
        _ =>
          EitherT.right(
            IO.delay(releaseFunctionWasCalled.set(true)) >> exception.raiseError[IO, Accepted]
          ),
        IO.delay(releaseFunctionWasCalled.set(false))
      ).unsafeRunSync() shouldBe SchedulingError(exception)

      eventually {
        releaseFunctionWasCalled.get shouldBe true
      }
    }

    "release the semaphore if the releaseProcess fails" in new TestCase {
      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO])
      (() => semaphore.acquire).expects().returning(().pure[IO])
      (() => semaphore.release).expects().returning(().pure[IO])
      val releaseFunctionWasCalled = new AtomicBoolean(false)

      tryExecuting(
        (deferred: Deferred[IO, Unit]) =>
          EitherT
            .rightT[IO, EventSchedulingResult](Accepted)
            .semiflatTap(_ => deferred.complete(())),
        IO.delay(releaseFunctionWasCalled.set(true)) >> exceptions.generateOne.raiseError[IO, Unit]
      ).unsafeRunSync() shouldBe Accepted

      eventually {
        releaseFunctionWasCalled.get shouldBe true
      }
    }

    "return Busy if there are no available spots" in new TestCase {
      (() => semaphore.available).expects().returning(0L.pure[IO])

      tryExecuting(
        _ => EitherT.rightT[IO, EventSchedulingResult](Accepted),
        IO.unit
      ).unsafeRunSync() shouldBe Busy
    }

    "throw an error if acquiring the semaphore fails and all spots are available" in new TestCase {
      val exception = exceptions.generateOne
      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO]).twice()
      (() => semaphore.acquire).expects().returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        tryExecuting(
          _ => EitherT.rightT[IO, EventSchedulingResult](Accepted),
          IO.unit
        ).unsafeRunSync()
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
        tryExecuting(
          _ => EitherT.rightT[IO, EventSchedulingResult](Accepted),
          IO.unit
        ).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
    }

    s"execute a process and return the result $Accepted" in new TestCase {
      withoutLimit
        .tryExecuting(resultWithoutLimit(EitherT.rightT(Accepted)))
        .unsafeRunSync() shouldBe Accepted
    }

    s"execute a process and return the result $BadRequest" in new TestCase {
      withoutLimit
        .tryExecuting(resultWithoutLimit(EitherT.leftT(BadRequest)))
        .unsafeRunSync() shouldBe BadRequest
    }

    "return a SchedulingError if the process fails" in new TestCase {

      val exception = exceptions.generateOne
      val process   = exception.raiseError[IO, Accepted]

      withoutLimit
        .tryExecuting(resultWithoutLimit(EitherT.right(process)))
        .unsafeRunSync() shouldBe SchedulingError(exception)
    }
  }

  private trait TestCase {

    private implicit val logger: TestLogger[IO] = TestLogger()
    val processesCount = positiveInts().generateOne
    val semaphore      = mock[TestSemaphore]

    private val limiter = new ConcurrentProcessesLimiterImpl[IO](processesCount, semaphore)
    val withoutLimit    = ConcurrentProcessExecutor.withoutLimit[IO]

    def resultWithoutLimit(result: EitherT[IO, EventSchedulingResult, Accepted]): EventHandlingDefinition[IO] =
      EventHandlingDefinition[IO](result).unsafeRunSync()

    def tryExecuting(process:        Deferred[IO, Unit] => EitherT[IO, EventSchedulingResult, Accepted],
                     releaseProcess: IO[Unit]
    ): IO[EventSchedulingResult] = for {
      handlerProcess <- EventHandlingDefinition.withWaitingForCompletion[IO](process, releaseProcess)
      result         <- limiter.tryExecuting(handlerProcess)
    } yield result
  }

  private trait TestSemaphore extends Semaphore[IO]
}
