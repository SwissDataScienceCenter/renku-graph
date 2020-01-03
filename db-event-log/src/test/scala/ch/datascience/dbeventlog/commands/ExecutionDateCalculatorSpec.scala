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

package ch.datascience.dbeventlog.commands

import java.time.Instant
import java.time.temporal.ChronoUnit._

import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog.EventStatus.{NonRecoverableFailure, TriplesStoreFailure}
import ch.datascience.dbeventlog.{CreatedDate, ExecutionDate}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ExecutionDateCalculatorSpec extends WordSpec with MockFactory with ScalaCheckPropertyChecks {

  s"newExecutionDate for $NonRecoverableFailure" should {

    "return current date as Execution Date" in new TestCase {
      forAll { (createdDate: CreatedDate, executionDate: ExecutionDate) =>
        executionDateCalculator.newExecutionDate[NonRecoverableFailure](createdDate, executionDate) shouldBe
          ExecutionDate(now)
      }
    }
  }

  s"newExecutionDate for $TriplesStoreFailure" should {

    "return current time + 1 min" in new TestCase {
      forAll(timestampsNotInTheFuture, nonNegativeInts()) { (timestamp, createdDateOffset) =>
        executionDateCalculator.newExecutionDate[TriplesStoreFailure](
          CreatedDate(timestamp minus (createdDateOffset.value, SECONDS)),
          ExecutionDate(timestamp)
        ) shouldBe ExecutionDate(now plus (1, MINUTES))
      }
    }
  }

  private trait TestCase {
    val getNow                  = mockFunction[Instant]
    val executionDateCalculator = new ExecutionDateCalculator(getNow)

    val now = Instant.now()
    getNow.expects().returning(now).atLeastOnce()
  }
}
