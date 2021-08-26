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

import cats.effect.concurrent.Deferred
import cats.syntax.all._
import ch.datascience.events.consumers.EventSchedulingResult._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{exceptions, nonEmptyStrings}
import ch.datascience.graph.model.events
import io.circe.literal.JsonStringContext
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class EventHandlerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "tryHandling" should {

    Set(Accepted, Busy, BadRequest, SchedulingError(exceptions.generateOne)).foreach { result =>
      s"return $result if handler can support the request" in new TestCase {
        val resultInContext = result.pure[Try]
        handleFunction.expects().returning(resultInContext)

        (processesLimiter.tryExecuting _)
          .expects((deferredDone -> resultInContext).pure[Try], None)
          .returning(resultInContext)

        handler.tryHandling(eventRequestContent) shouldBe result.pure[Try]
      }
    }

    s"return $UnsupportedEventType if handler cannot support the request" in new TestCase {
      val unsupportedEvent = EventRequestContent(
        json"""{ "categoryName": ${nonEmptyStrings().generateOne} }""",
        nonEmptyStrings().generateOption
      )

      handler.tryHandling(unsupportedEvent) shouldBe UnsupportedEventType.pure[Try]
    }
  }

  private trait TestCase {
    val processesLimiter = mock[ConcurrentProcessesLimiter[Try]]
    val anyCategoryName  = nonEmptyStrings().generateOne
    val handleFunction   = mockFunction[Try[EventSchedulingResult]]
    val deferredDone     = mock[Deferred[Try, Unit]]

    val eventRequestContent =
      EventRequestContent(json"""{ "categoryName": $anyCategoryName }""", nonEmptyStrings().generateOption)
    val handler: EventHandler[Try] = new EventHandlerWithProcessLimiter[Try](processesLimiter) {
      override val categoryName: events.CategoryName = anyCategoryName

      protected override def handle(
          request: EventRequestContent
      ): Try[(Deferred[Try, Unit], Try[EventSchedulingResult])] =
        (deferredDone -> handleFunction()).pure[Try]
    }
  }
}
