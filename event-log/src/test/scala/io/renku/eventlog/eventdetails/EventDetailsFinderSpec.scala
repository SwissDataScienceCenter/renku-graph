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

package io.renku.eventlog.eventdetails

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.compoundEventIds
import io.renku.graph.model.events.EventDetails
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class EventDetailsFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  "findDetails" should {

    "return the details of the event if found" in testDBResource.use { implicit cfg =>
      for {
        event <- storeGeneratedEvent()
        _ <- eventDetailsFinder
               .findDetails(event.eventId)
               .asserting(_ shouldBe EventDetails(event.eventId, event.body).some)
      } yield Succeeded
    }

    "return None if the event is not found" in testDBResource.use { implicit cfg =>
      eventDetailsFinder.findDetails(compoundEventIds.generateOne).asserting(_ shouldBe None)
    }
  }

  private def eventDetailsFinder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new EventDetailsFinderImpl[IO]
  }
}
