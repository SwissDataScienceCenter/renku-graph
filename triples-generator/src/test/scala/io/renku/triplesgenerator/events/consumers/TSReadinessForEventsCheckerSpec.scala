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

package io.renku.triplesgenerator.events.consumers

import cats.effect.IO
import cats.syntax.all._
import io.renku.events.consumers.EventSchedulingResult.{SchedulingError, ServiceUnavailable}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.TSStateChecker.TSState
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class TSReadinessForEventsCheckerSpec
    extends AnyWordSpec
    with should.Matchers
    with TableDrivenPropertyChecks
    with IOSpec
    with MockFactory {

  "verifyTSReady" should {
    forAll(
      Table(
        "TSState"               -> "Result",
        TSState.ReProvisioning  -> ServiceUnavailable("Re-provisioning running"),
        TSState.MissingDatasets -> ServiceUnavailable("Not all datasets created")
      )
    ) { case (tsState, result) =>
      show"return ServiceUnavailable if TSState is $tsState" in new TestCase {
        (() => tsStateChecker.checkTSState).expects().returning(tsState.pure[IO])

        checker.verifyTSReady.value.unsafeRunSync() shouldBe result.asLeft
      }
    }

    "return SchedulingError if TSState check fails" in new TestCase {
      val exception = exceptions.generateOne
      (() => tsStateChecker.checkTSState).expects().returning(exception.raiseError[IO, TSState])

      checker.verifyTSReady.value.unsafeRunSync() shouldBe SchedulingError(exception).asLeft
    }
  }

  private trait TestCase {
    val tsStateChecker = mock[TSStateChecker[IO]]
    val checker        = new TSReadinessForEventsCheckerImpl(tsStateChecker)
  }
}
