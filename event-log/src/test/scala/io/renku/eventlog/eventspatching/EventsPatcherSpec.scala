/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.eventspatching

import cats.effect.IO
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import doobie.implicits._
import io.renku.eventlog.EventStatus._
import io.renku.eventlog._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventsPatcherSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  "applyToAllEvents" should {

    "execute given patch's query and update it's gauges" in new TestCase {

      (patch.name _)
        .expects()
        .returning(patchName)

      (patch.query _)
        .expects()
        .returning(sql"update event_log set status = ${New: EventStatus}")

      (patch.updateGauges _)
        .expects()
        .returning(IO.unit)

      patcher.applyToAllEvents(patch).unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info(s"All events patched with $patchName"))
    }

    "log a failure when running the update fails" in new TestCase {

      (patch.name _)
        .expects()
        .returning(patchName)

      (patch.query _)
        .expects()
        .returning(sql"update nonexisting set status = ${New: EventStatus}")

      val exception = intercept[Exception] {
        patcher.applyToAllEvents(patch).unsafeRunSync()
      }

      val errorMessage = s"Patching all events with $patchName failed"
      exception.getMessage shouldBe errorMessage
      logger.loggedOnly(Error(errorMessage, exception.getCause))
    }

    "log a failure when updating gauges fails" in new TestCase {

      (patch.name _)
        .expects()
        .returning(patchName)

      (patch.query _)
        .expects()
        .returning(sql"update event_log set status = ${New: EventStatus}")

      val exception = exceptions.generateOne
      (patch.updateGauges _)
        .expects()
        .returning(exception.raiseError[IO, Unit])

      val actualException = intercept[Exception] {
        patcher.applyToAllEvents(patch).unsafeRunSync()
      }

      val errorMessage = s"Patching all events with $patchName failed"
      actualException.getMessage shouldBe errorMessage

      actualException.getCause shouldBe exception

      logger.loggedOnly(Error(errorMessage, exception))
    }
  }

  private trait TestCase {

    val patchName = nonBlankStrings().generateOne
    val patch     = mock[EventsPatch[IO]]

    val logger  = TestLogger[IO]()
    val patcher = new EventsPatcher(transactor, logger)
  }
}
