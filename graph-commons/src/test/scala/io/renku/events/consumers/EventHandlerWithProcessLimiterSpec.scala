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

import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.events.Generators.{categoryNames, eventRequestContents}
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.generators.Generators.{exceptions, ints, nonEmptyStrings}
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import ConsumersModelGenerators.notHappySchedulingResults

class EventHandlerWithProcessLimiterSpec extends AnyWordSpec with IOSpec with should.Matchers {

  "tryHandling" should {

    "return the precondition if defined" in new TestCase {

      val result = ServiceUnavailable(nonEmptyStrings().generateOne)
      override val precondition: IO[Option[EventSchedulingResult]] = result.some.pure[IO]

      val request = json"""{"categoryName": $category}"""

      handler.tryHandling(EventRequestContent(request)).unsafeRunSync() shouldBe result
    }

    "return UnsupportedEventType if category of the event does not match the handler category" in new TestCase {
      handler.tryHandling(eventRequestContents.generateOne).unsafeRunSync() shouldBe UnsupportedEventType
    }

    "return BadRequest in case decoding fails" in new TestCase {

      override val precondition: IO[Option[EventSchedulingResult]] = None.pure[IO]
      val exception = exceptions.generateOne
      override val decode: EventRequestContent => Either[Exception, Int] = _ => exception.asLeft

      val request = json"""{"categoryName": $category}"""

      handler.tryHandling(EventRequestContent(request)).unsafeRunSync() shouldBe BadRequest(exception.getMessage)
    }

    "return result the process executor returns in case the event gets scheduled" in new TestCase {

      override val precondition: IO[Option[EventSchedulingResult]] = None.pure[IO]
      val event = ints().generateOne
      override val decode: EventRequestContent => Either[Exception, Int] = _ => event.asRight
      val processInput = Ref.unsafe[IO, Option[Int]](None)
      override val process:   Int => IO[Unit]  = v => processInput.set(v.some)
      override val onRelease: Option[IO[Unit]] = processInput.update(_.map(_ + 1)).some

      val request = json"""{"categoryName": $category}"""

      handler.tryHandling(EventRequestContent(request)).unsafeRunSync() shouldBe successExecutionResult

      processInput.get.unsafeRunSync() shouldBe (event + 1).some
    }

    "run post handling hook" in new TestCase {
      val ref = Ref.unsafe[IO, Option[(Int, EventSchedulingResult)]](None)

      val event = ints().generateOne
      override val decode:  EventRequestContent => Either[Exception, Int] = _ => event.asRight
      override val process: Int => IO[Unit]                               = _ => IO.unit

      override def postProcess(event: Int, result: EventSchedulingResult): IO[Unit] =
        ref.set(Some(event -> result))

      override val precondition: IO[Option[EventSchedulingResult]] = IO.pure(None)
      override val onRelease:    Option[IO[Unit]]                  = None

      val request = json"""{"categoryName": $category}"""

      handler.tryHandling(EventRequestContent(request)).unsafeRunSync()

      ref.get.unsafeRunSync() shouldBe Some(event -> EventSchedulingResult.Accepted)
    }

    "handle the process failure and still execute on onRelease procedure" in new TestCase {

      override val precondition: IO[Option[EventSchedulingResult]]             = None.pure[IO]
      override val decode:       EventRequestContent => Either[Exception, Int] = _ => ints().generateOne.asRight
      val processInput = Ref.unsafe[IO, Option[Int]](None)
      override val process: Int => IO[Unit] = _ => exceptions.generateOne.raiseError[IO, Unit]
      val onReleaseValue = ints().generateSome
      override val onRelease: Option[IO[Unit]] = processInput.set(onReleaseValue).some

      val request = json"""{"categoryName": $category}"""

      handler.tryHandling(EventRequestContent(request)).unsafeRunSync() shouldBe failureExecutionResult

      processInput.get.unsafeRunSync() shouldBe onReleaseValue
    }

    "handle the onRelease failure" in new TestCase {

      override val precondition: IO[Option[EventSchedulingResult]] = None.pure[IO]
      val event = ints().generateOne
      override val decode: EventRequestContent => Either[Exception, Int] = _ => event.asRight
      val processInput = Ref.unsafe[IO, Option[Int]](None)
      override val process:   Int => IO[Unit]  = v => processInput.set(v.some)
      override val onRelease: Option[IO[Unit]] = exceptions.generateOne.raiseError[IO, Unit].some

      val request = json"""{"categoryName": $category}"""

      handler.tryHandling(EventRequestContent(request)).unsafeRunSync() shouldBe failureExecutionResult

      processInput.get.unsafeRunSync() shouldBe event.some
    }
  }

  private trait TestCase {

    val category = categoryNames.generateOne
    val decode:  EventRequestContent => Either[Exception, Int] = _ => throw exceptions.generateOne
    val process: Int => IO[Unit]                               = _ => throw exceptions.generateOne
    val precondition: IO[Option[EventSchedulingResult]] =
      exceptions.generateOne.raiseError[IO, Option[EventSchedulingResult]]
    val onRelease: Option[IO[Unit]] = Some(exceptions.generateOne.raiseError[IO, Unit])

    @annotation.nowarn("cat=unused")
    def postProcess(event: Int, result: EventSchedulingResult): IO[Unit] = IO.unit

    implicit val logger: TestLogger[IO] = TestLogger[IO]()

    val successExecutionResult = Accepted
    val failureExecutionResult = notHappySchedulingResults.generateOne
    private val processExecutor = new ProcessExecutor[IO] {
      override def tryExecuting(process: IO[Unit]): IO[EventSchedulingResult] =
        process.as(successExecutionResult).recover(_ => failureExecutionResult)
    }
    lazy val handler = new EventHandlerWithProcessLimiter[IO](processExecutor) {

      override val categoryName: CategoryName = category
      protected override type Event = Int

      protected override def onPostHandling(event: Int, result: EventSchedulingResult): IO[Unit] =
        postProcess(event, result)

      protected override def createHandlingDefinition(): EventHandlingDefinition = EventHandlingDefinition(
        decode,
        process,
        precondition,
        onRelease
      )
    }
  }
}
