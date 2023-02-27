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

import cats.effect.{IO, Ref, Temporal}
import cats.syntax.all._
import io.renku.events.consumers.EventSchedulingResult.{Accepted, Busy}
import io.renku.generators.Generators.{exceptions, ints, positiveInts}
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class ConcurrentProcessExecutorSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  "tryExecuting" should {

    "return Accepted and execute the process if there are available spots; " +
      "otherwise return Busy" in new TestCase {

        val schedulableValues = (1 to processesCount.value).toList
        val schedulable       = schedulableValues map { v => executor.tryExecuting(slowProcess(v)) }

        val nonSchedulableValue = ints(processesCount.value + 1).generateOne
        val nonSchedulable      = executor.tryExecuting(slowProcess(nonSchedulableValue))

        (schedulable :+ nonSchedulable).sequence
          .unsafeRunSync()
          .distinct should contain theSameElementsAs Set(Accepted, Busy)

        eventually {
          executionRegister.get.unsafeRunSync() should contain theSameElementsAs schedulableValues
        }
      }

    "be able to schedule a next job once there's a spot available again" in new TestCase {

      val schedulableValues = (1 to processesCount.value).toList
      val schedulable       = schedulableValues map { v => executor.tryExecuting(slowProcess(v)) }

      val nonSchedulableValue = ints(processesCount.value + 1).generateOne
      val nonSchedulable      = executor.tryExecuting(slowProcess(nonSchedulableValue))

      (schedulable :+ nonSchedulable).sequence
        .unsafeRunSync()
        .distinct should contain theSameElementsAs Set(Accepted, Busy)

      eventually {
        executionRegister.get.unsafeRunSync().isEmpty shouldBe false
      }

      executor.tryExecuting(slowProcess(nonSchedulableValue)).unsafeRunSync() shouldBe Accepted

      eventually {
        executionRegister.get.unsafeRunSync() should contain theSameElementsAs nonSchedulableValue :: schedulableValues
      }
    }

    "clear an available spot in case of a failing process" in new TestCase {

      val schedulableValues = (1 until processesCount.value).toList
      val schedulable       = schedulableValues map { v => executor.tryExecuting(slowProcess(v)) }

      val failingSchedulable = executor.tryExecuting(failingProcess)

      (failingSchedulable :: schedulable).sequence
        .unsafeRunSync()
        .distinct shouldBe List(Accepted)

      eventually {
        executionRegister.get.unsafeRunSync() should contain theSameElementsAs schedulableValues
      }

      executionRegister.set(Nil).unsafeRunSync()

      val anotherValue = ints(processesCount.value + 1).generateOne
      eventually {
        executor.tryExecuting(slowProcess(anotherValue)).unsafeRunSync() shouldBe Accepted
      }

      eventually {
        executionRegister.get.unsafeRunSync() shouldBe List(anotherValue)
      }
    }

//    "run the releaseProcess and return a SchedulingError if the process fails" in new TestCase {
//      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO])
//      (() => semaphore.acquire).expects().returning(().pure[IO])
//      (() => semaphore.release).expects().returning(().pure[IO])
//
//      val releaseFunctionWasCalled = new AtomicBoolean(false)
//
//      val exception = exceptions.generateOne
//
//      tryExecuting(
//        _ =>
//          EitherT.right(
//            IO.delay(releaseFunctionWasCalled.set(true)) >> exception.raiseError[IO, Accepted]
//          ),
//        IO.delay(releaseFunctionWasCalled.set(false))
//      ).unsafeRunSync() shouldBe SchedulingError(exception)
//
//      eventually {
//        releaseFunctionWasCalled.get shouldBe true
//      }
//    }
//
//    "release the semaphore if the releaseProcess fails" in new TestCase {
//      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO])
//      (() => semaphore.acquire).expects().returning(().pure[IO])
//      (() => semaphore.release).expects().returning(().pure[IO])
//      val releaseFunctionWasCalled = new AtomicBoolean(false)
//
//      tryExecuting(
//        (deferred: Deferred[IO, Unit]) =>
//          EitherT
//            .rightT[IO, EventSchedulingResult](Accepted)
//            .semiflatTap(_ => deferred.complete(())),
//        IO.delay(releaseFunctionWasCalled.set(true)) >> exceptions.generateOne.raiseError[IO, Unit]
//      ).unsafeRunSync() shouldBe Accepted
//
//      eventually {
//        releaseFunctionWasCalled.get shouldBe true
//      }
//    }
//
//    "return Busy if there are no available spots" in new TestCase {
//      (() => semaphore.available).expects().returning(0L.pure[IO])
//
//      tryExecuting(
//        _ => EitherT.rightT[IO, EventSchedulingResult](Accepted),
//        IO.unit
//      ).unsafeRunSync() shouldBe Busy
//    }
//
//    "throw an error if acquiring the semaphore fails and all spots are available" in new TestCase {
//      val exception = exceptions.generateOne
//      (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO]).twice()
//      (() => semaphore.acquire).expects().returning(exception.raiseError[IO, Unit])
//
//      intercept[Exception] {
//        tryExecuting(
//          _ => EitherT.rightT[IO, EventSchedulingResult](Accepted),
//          IO.unit
//        ).unsafeRunSync()
//      }.getMessage shouldBe exception.getMessage
//    }
//
//    "throw an error if acquiring the semaphore fails and release the semaphore if some spots are not available" in new TestCase {
//      val exception = exceptions.generateOne
//      inSequence {
//        (() => semaphore.available).expects().returning(processesCount.value.toLong.pure[IO]).once()
//        (() => semaphore.acquire).expects().returning(exception.raiseError[IO, Unit])
//        (() => semaphore.available).expects().returning(0.toLong.pure[IO]).once()
//        (() => semaphore.release).expects().returning(().pure[IO])
//      }
//
//      intercept[Exception] {
//        tryExecuting(
//          _ => EitherT.rightT[IO, EventSchedulingResult](Accepted),
//          IO.unit
//        ).unsafeRunSync()
//      }.getMessage shouldBe exception.getMessage
//    }
//
//    s"execute a process and return the result $Accepted" in new TestCase {
//      withoutLimit
//        .tryExecuting(resultWithoutLimit(EitherT.rightT(Accepted)))
//        .unsafeRunSync() shouldBe Accepted
//    }
//
//    s"execute a process and return the result $BadRequest" in new TestCase {
//      withoutLimit
//        .tryExecuting(resultWithoutLimit(EitherT.leftT(BadRequest)))
//        .unsafeRunSync() shouldBe BadRequest
//    }
//
//    "return a SchedulingError if the process fails" in new TestCase {
//
//      val exception = exceptions.generateOne
//      val process   = exception.raiseError[IO, Accepted]
//
//      withoutLimit
//        .tryExecuting(resultWithoutLimit(EitherT.right(process)))
//        .unsafeRunSync() shouldBe SchedulingError(exception)
//    }
  }

  private trait TestCase {

    val executionRegister = Ref.unsafe[IO, List[Int]](List.empty)

    val slowProcess:    Int => IO[Unit] = v => Temporal[IO].delayBy(executionRegister.update(v :: _), 500 millis)
    val failingProcess: IO[Unit]        = Temporal[IO].delayBy(exceptions.generateOne.raiseError[IO, Unit], 250 millis)

    val processesCount = positiveInts(max = 3).generateOne
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val executor = ProcessExecutor.concurrent[IO](processesCount).unsafeRunSync()
  }
}
