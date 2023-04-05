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
import scala.util.Random

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

        schedulable.parSequence
          .unsafeRunSync()
          .distinct
          .toSet                       shouldBe Set(Accepted)
        nonSchedulable.unsafeRunSync() shouldBe Busy

        eventually {
          executionRegister.get.unsafeRunSync() should contain theSameElementsAs schedulableValues
        }
      }

    "be able to schedule a next job once there's a spot available again" in new TestCase {

      val schedulableValues = (1 to processesCount.value).toList
      val schedulable       = schedulableValues map { v => executor.tryExecuting(slowProcess(v)) }

      val nonSchedulableValue = ints(processesCount.value + 1).generateOne
      val nonSchedulable      = executor.tryExecuting(slowProcess(nonSchedulableValue))

      schedulable.parSequence
        .unsafeRunSync()
        .distinct
        .toSet                       shouldBe Set(Accepted)
      nonSchedulable.unsafeRunSync() shouldBe Busy

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

      Random
        .shuffle(failingSchedulable :: schedulable)
        .parSequence
        .unsafeRunSync()
        .distinct shouldBe List(Accepted)

      eventually {
        executionRegister.get.unsafeRunSync() should contain theSameElementsAs schedulableValues
      }
    }
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
