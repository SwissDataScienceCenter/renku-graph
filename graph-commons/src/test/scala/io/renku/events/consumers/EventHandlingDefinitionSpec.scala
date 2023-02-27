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

//import cats.data.EitherT
//import cats.effect.{Deferred, IO, Ref}
//import cats.syntax.all._
//import io.renku.events.consumers.EventSchedulingResult.Accepted
//import io.renku.generators.Generators.Implicits._
//import io.renku.generators.Generators.exceptions
//import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlingDefinitionSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with Eventually
    with IntegrationPatience {

//  "withWaitingForCompletion" should {
//
//    "complete the deferred process flag in case of success" in new TestCase {
//
//      val underlyingProcess = givenProcess(returning = EitherT.rightT[IO, EventSchedulingResult](Accepted))
//
//      val handlingProcess = EventHandlingDefinition
//        .withWaitingForCompletion[IO](underlyingProcess, releaseProcess = ().pure[IO])
//        .unsafeRunSync()
//
//      handlingProcess.process.value.unsafeRunAndForget()
//
//      givenProcessFinishedFlagSet(onCompletionOf = handlingProcess)
//
//      eventually {
//        processFinished.get.unsafeRunSync() shouldBe true
//      }
//    }
//
//    "complete the deferred process flag in case of a Left" in new TestCase {
//
//      val underlyingProcess = givenProcess(
//        returning = EitherT.leftT[IO, Accepted](EventSchedulingResult.SchedulingError(exceptions.generateOne))
//      )
//
//      val handlingProcess = EventHandlingDefinition
//        .withWaitingForCompletion[IO](underlyingProcess, releaseProcess = ().pure[IO])
//        .unsafeRunSync()
//
//      handlingProcess.process.value.unsafeRunAndForget()
//
//      givenProcessFinishedFlagSet(onCompletionOf = handlingProcess)
//
//      eventually {
//        processFinished.get.unsafeRunSync() shouldBe true
//      }
//    }
//
//    "complete the deferred process flag in case of a failure" in new TestCase {
//
//      val underlyingProcess = givenProcess(
//        returning = EitherT.right[EventSchedulingResult](exceptions.generateOne.raiseError[IO, Accepted])
//      )
//
//      val handlingProcess = EventHandlingDefinition
//        .withWaitingForCompletion[IO](underlyingProcess, releaseProcess = ().pure[IO])
//        .unsafeRunSync()
//
//      handlingProcess.process.value.unsafeRunAndForget()
//
//      givenProcessFinishedFlagSet(onCompletionOf = handlingProcess)
//
//      eventually {
//        processFinished.get.unsafeRunSync() shouldBe true
//      }
//    }
//
//    "turn failure occurring during process execution to SchedulingError" in new TestCase {
//
//      val exception = exceptions.generateOne
//      val underlyingProcess = givenProcess(
//        returning = EitherT.right[EventSchedulingResult](exception.raiseError[IO, Accepted])
//      )
//
//      val handlingProcess = EventHandlingDefinition
//        .withWaitingForCompletion[IO](underlyingProcess, releaseProcess = ().pure[IO])
//        .unsafeRunSync()
//
//      handlingProcess.process.value.unsafeRunSync() shouldBe EventSchedulingResult.SchedulingError(exception).asLeft
//    }
//  }
//
//  private trait TestCase {
//
//    implicit val logger: TestLogger[IO] = TestLogger()
//
//    val processFinished = Ref.unsafe[IO, Boolean](false)
//
//    def givenProcess(returning: EitherT[IO, EventSchedulingResult, Accepted]) = { (dfrd: Deferred[IO, Unit]) =>
//      returning
//        .semiflatTap(_ => dfrd.complete(()))
//    }
//
//    def givenProcessFinishedFlagSet(onCompletionOf: EventHandlingDefinition[IO]): Unit =
//      (onCompletionOf.waitToFinish() >> processFinished.set(true)).unsafeRunAndForget()
//  }
}
