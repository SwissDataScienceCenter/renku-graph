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

package io.renku.eventlog.eventspatching

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus.New
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.metrics.{SingleValueGauge, TestLabeledHistogram}
import eu.timepit.refined.auto._
import io.renku.eventlog._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.data.Completion
import skunk.implicits._

class EventsPatcherSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "applyToAllEvents" should {

    "execute given patch's query and update it's gauges" in new TestCase {

      val patch = TestEventsPatch(gauge)

      (gauge.set _).expects(*).returning(IO.unit)

      patcher.applyToAllEvents(patch).unsafeRunSync() shouldBe ((): Unit)

      queriesExecTimes.verifyExecutionTimeMeasured(patch.query.name)

      logger.loggedOnly(Info(s"All events patched with ${patch.name}"))
    }

    "log a failure when running the update fails" in new TestCase {

      val patch =
        TestEventsPatch(gauge,
                        Kleisli[IO, Session[IO], Completion](session => session.execute(sql"UPDATE event".command))
        )

      val exception = intercept[Exception] {
        patcher.applyToAllEvents(patch).unsafeRunSync()
      }

      val errorMessage = s"Patching all events with ${patch.name} failed"
      exception.getMessage shouldBe errorMessage
      logger.loggedOnly(Error(errorMessage, exception.getCause))

      queriesExecTimes.verifyExecutionTimeMeasured(patch.query.name)
    }

    "log a failure when updating gauges fails" in new TestCase {

      val patch = TestEventsPatch(gauge)

      val exception = exceptions.generateOne
      (gauge.set _).expects(*).returning(exception.raiseError[IO, Unit])

      val actualException = intercept[Exception] {
        patcher.applyToAllEvents(patch).unsafeRunSync()
      }

      val errorMessage = s"Patching all events with ${patch.name} failed"
      actualException.getMessage shouldBe errorMessage

      actualException.getCause shouldBe exception

      logger.loggedOnly(Error(errorMessage, exception))

      queriesExecTimes.verifyExecutionTimeMeasured(patch.query.name)
    }
  }

  private trait TestCase {

    val gauge = mock[SingleValueGauge[IO]]

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val logger           = TestLogger[IO]()
    val patcher          = new EventsPatcherImpl(sessionResource, queriesExecTimes, logger)
  }

  private case class TestEventsPatch(
      gauge: SingleValueGauge[IO],
      protected val sqlQuery: Kleisli[IO, Session[IO], Completion] = Kleisli { session =>
        val query: Command[EventStatus] = sql"""UPDATE event
                                                SET status = $eventStatusEncoder
                                                """.command
        session.prepare(query).use(_.execute(New))
      }
  ) extends EventsPatch[IO] {
    override val name           = "test events patch"
    override def updateGauges() = gauge.set(20d)
  }
}
