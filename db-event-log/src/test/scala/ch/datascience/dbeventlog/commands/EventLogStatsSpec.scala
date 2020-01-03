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

import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog._
import EventStatus._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EventLogStatsSpec extends WordSpec with InMemoryEventLogDbSpec with ScalaCheckPropertyChecks {

  "statuses" should {

    "return info about number of events grouped by status" in {
      forAll { statuses: List[EventStatus] =>
        prepareDbForTest()

        statuses foreach store

        stats.statuses.unsafeRunSync() shouldBe Map(
          New                   -> statuses.count(_ == New),
          Processing            -> statuses.count(_ == Processing),
          TriplesStore          -> statuses.count(_ == TriplesStore),
          TriplesStoreFailure   -> statuses.count(_ == TriplesStoreFailure),
          NonRecoverableFailure -> statuses.count(_ == NonRecoverableFailure)
        )
      }
    }
  }

  private val stats = new EventLogStats(transactor)

  private def store(status: EventStatus): Unit =
    storeEvent(commitEventIds.generateOne,
               status,
               executionDates.generateOne,
               committedDates.generateOne,
               eventBodies.generateOne)
}
