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

package io.renku.eventlog.events.consumers.statuschange.rollbacktonew

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.eventlog.EventLogPostgresSpec
import io.renku.eventlog.api.events.StatusChangeEvent.RollbackToNew
import io.renku.eventlog.events.consumers.statuschange.DBUpdateResults
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class DbUpdaterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  "updateDB" should {

    s"change the status of the given event from $GeneratingTriples to $New" in testDBResource.use { implicit cfg =>
      for {
        event      <- storeGeneratedEvent(GeneratingTriples)
        otherEvent <- storeGeneratedEvent(GeneratingTriples)

        _ <- moduleSessionResource.session
               .useKleisli(dbUpdater.updateDB(RollbackToNew(event.eventId.id, event.project)))
               .asserting(_ shouldBe DBUpdateResults(event.project.slug, GeneratingTriples -> -1, New -> 1))

        _ <- findEvent(event.eventId).asserting(_.map(_.status) shouldBe Some(New))
        _ <- findEvent(otherEvent.eventId).asserting(_.map(_.status) shouldBe Some(GeneratingTriples))
      } yield Succeeded
    }

    s"do nothing if event is not in the $GeneratingTriples status" in testDBResource.use { implicit cfg =>
      val invalidStatus = Gen.oneOf(EventStatus.all.filterNot(_ == GeneratingTriples)).generateOne
      for {
        event <- storeGeneratedEvent(invalidStatus)

        _ <- moduleSessionResource.session
               .useKleisli(dbUpdater.updateDB(RollbackToNew(event.eventId.id, event.project)))
               .asserting(_ shouldBe DBUpdateResults.empty)

        _ <- findEvent(event.eventId).asserting(_.map(_.status) shouldBe Some(invalidStatus))
      } yield Succeeded
    }
  }

  private lazy val now = Instant.now()
  private lazy val dbUpdater = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new DbUpdater[IO](() => now)
  }
}
