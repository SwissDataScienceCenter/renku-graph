/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import java.lang.Math.log
import java.time.temporal.ChronoUnit._
import java.time.{Duration, Instant}

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

    s"return current time + 10s if Execution Date == Created Date" in new TestCase {
      forAll(timestampsNotInTheFuture) { timestamp =>
        executionDateCalculator.newExecutionDate[TriplesStoreFailure](
          CreatedDate(timestamp),
          ExecutionDate(timestamp)
        ) shouldBe ExecutionDate(now plus (10, SECONDS))
      }
    }

    "return current time + 10s " +
      "if Execution Date != Created Date " +
      "and Execution Date + (Execution Date - Created Date) * log(Execution Date - Created Date) <= now" in new TestCase {

      executionDateCalculator.newExecutionDate[TriplesStoreFailure](
        CreatedDate(now minus (1000, SECONDS)),
        ExecutionDate(now minus (1000 - 10, SECONDS))
      ) shouldBe ExecutionDate(now plus (10, SECONDS))
    }

    "return Execution Date + (Execution Date - Created Date) * log(Execution Date - Created Date) " +
      "if Execution Date + (Execution Date - Created Date) * log(Execution Date - Created Date) > now" in new TestCase {
      val createdDate   = CreatedDate(now minus (1000, SECONDS))
      val executionDate = ExecutionDate(now minus (1, SECONDS))

      val diffInSeconds = Duration.between(createdDate.value, executionDate.value).getSeconds
      val addend        = diffInSeconds * log(diffInSeconds).toLong

      executionDateCalculator.newExecutionDate[TriplesStoreFailure](
        createdDate,
        executionDate
      ) shouldBe ExecutionDate(executionDate.value plus (addend, SECONDS))
    }
  }

  private trait TestCase {
    val getNow                  = mockFunction[Instant]
    val executionDateCalculator = new ExecutionDateCalculator(getNow)

    val now = Instant.now()
    getNow.expects().returning(now).atLeastOnce()
  }
}
