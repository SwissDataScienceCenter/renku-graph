/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import cats.syntax.all._
import io.circe.literal.JsonStringContext
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.events.Generators.categoryNames
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with should.Matchers with MockFactory {

  "tryHandling" should {

    s"return $Accepted if handler can support the request" in new TestCase {

      (for {
        process <- EventHandlingProcess[IO](EitherT.rightT[IO, EventSchedulingResult](Accepted))
        _ <- (processesLimiter.tryExecuting _)
               .expects(process)
               .returning(Accepted.pure[IO])
               .pure[IO]
        result <-
          handlerWithProcess(process).tryHandling(eventRequestContent)
      } yield result).unsafeRunSync() shouldBe Accepted
    }

    Set(BadRequest, SchedulingError(exceptions.generateOne)).foreach { result =>
      s"return $result if handler can support the request but an error occurs" in new TestCase {
        (for {
          process <- EventHandlingProcess[IO](EitherT.leftT[IO, Accepted](result))
          _ <- (processesLimiter.tryExecuting _)
                 .expects(process)
                 .returning(result.pure[IO])
                 .pure[IO]
          result <-
            handlerWithProcess(process).tryHandling(eventRequestContent)
        } yield result).unsafeRunSync() shouldBe result
      }
    }

    s"return $UnsupportedEventType if handler cannot support the request" in new TestCase {
      val unsupportedEvent = EventRequestContent.NoPayload(
        json"""{ "categoryName": ${nonEmptyStrings().generateOne} }"""
      )

      (for {
        process <- EventHandlingProcess[IO](EitherT.rightT[IO, EventSchedulingResult](Accepted))
        result  <- handlerWithProcess(process).tryHandling(unsupportedEvent)
      } yield result).unsafeRunSync() shouldBe UnsupportedEventType
    }
  }

  private trait TestCase {
    val processesLimiter = mock[ConcurrentProcessesLimiter[IO]]
    val anyCategoryName  = categoryNames.generateOne

    val eventRequestContent = EventRequestContent.NoPayload(
      json"""{ "categoryName": $anyCategoryName }"""
    )

    def handlerWithProcess(process: EventHandlingProcess[IO]): EventHandlerWithProcessLimiter[IO] =
      new EventHandlerWithProcessLimiter[IO](processesLimiter) {
        override val categoryName: CategoryName = anyCategoryName

        protected override def createHandlingProcess(request: EventRequestContent): IO[EventHandlingProcess[IO]] =
          IO(process)
      }
  }
}
