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

package io.renku.eventlog.subscriptions.awaitinggeneration

import cats.effect.{IO, Timer}
import cats.syntax.all._
import ch.datascience.events.consumers.subscriptions._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.events.EventStatus.GenerationNonRecoverableFailure
import ch.datascience.graph.model.projects
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.metrics.LabeledGauge
import io.renku.eventlog.statuschange.StatusUpdatesRunner
import io.renku.eventlog.statuschange.commands.UpdateResult.Updated
import io.renku.eventlog.statuschange.commands.{ToGenerationNonRecoverableFailure, UpdateResult}
import org.scalamock.matchers.ArgCapture.CaptureAll
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class DispatchRecoverySpec extends AnyWordSpec with should.Matchers with MockFactory {

  "recovery" should {

    "retry changing event status if status update failed initially" in new TestCase {

      val event      = awaitingGenerationEvents.generateOne
      val exception  = exceptions.generateOne
      val subscriber = subscriberUrls.generateOne

      val nonRecoverableStatusUpdate = CaptureAll[ToGenerationNonRecoverableFailure[IO]]()

      (statusUpdateRunner.run _)
        .expects(capture(nonRecoverableStatusUpdate))
        .returning(exception.raiseError[IO, UpdateResult])

      // retrying
      (statusUpdateRunner.run _)
        .expects(capture(nonRecoverableStatusUpdate))
        .returning(Updated.pure[IO])

      dispatchRecovery.recover(subscriber, event)(exception).unsafeRunSync() shouldBe ((): Unit)

      nonRecoverableStatusUpdate.value.eventId                     shouldBe event.id
      nonRecoverableStatusUpdate.value.underTriplesGenerationGauge shouldBe underTriplesGenerationGauge
      nonRecoverableStatusUpdate.value.message.value                 should include(exception.getMessage)

      logger.loggedOnly(
        Error(s"${SubscriptionCategory.name}: Marking event as $GenerationNonRecoverableFailure failed", exception),
        Error(s"${SubscriptionCategory.name}: $event, url = $subscriber -> $GenerationNonRecoverableFailure", exception)
      )
    }
  }

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  private trait TestCase {
    val underTriplesGenerationGauge = mock[LabeledGauge[IO, projects.Path]]
    val statusUpdateRunner          = mock[StatusUpdatesRunner[IO]]
    val logger                      = TestLogger[IO]()
    val dispatchRecovery = new DispatchRecoveryImpl[IO](
      underTriplesGenerationGauge,
      statusUpdateRunner,
      logger,
      onErrorSleep = 100 millis
    )
  }
}
