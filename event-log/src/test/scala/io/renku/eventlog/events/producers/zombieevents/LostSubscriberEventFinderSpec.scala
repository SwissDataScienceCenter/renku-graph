/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers.zombieevents

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.Generators._
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus.ProcessingStatus
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class LostSubscriberEventFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  "popEvent" should {

    ProcessingStatus.all foreach { status =>
      "return event which belongs to a subscriber not listed in the subscriber table " +
        s"if event's status is $status" in testDBResource.use { implicit cfg =>
          val project = consumerProjects.generateOne
          for {
            event <- addEvent(status, project)
            _     <- upsertEventDelivery(event.eventId, subscriberId)

            activeSubscriberId  = subscriberIds.generateOne
            activeSubscriberUrl = subscriberUrls.generateOne
            notLostEvent <- addEvent(status, project)
            _            <- upsertSubscriber(activeSubscriberId, activeSubscriberUrl, sourceUrl)
            _            <- upsertEventDelivery(notLostEvent.eventId, activeSubscriberId)

            _ <- finder
                   .popEvent()
                   .asserting(_ shouldBe ZombieEvent(finder.processName, event.eventId, project.slug, status).some)
            _ <- finder.popEvent().asserting(_ shouldBe None)
          } yield Succeeded
        }
    }

    EventStatus.all diff ProcessingStatus.all.asInstanceOf[Set[EventStatus]] foreach { status =>
      s"return None when the event has a the status $status" in testDBResource.use { implicit cfg =>
        val project = consumerProjects.generateOne
        for {
          event <- addEvent(status, project)
          _     <- upsertEventDelivery(event.eventId, subscriberId)

          _ <- finder.popEvent().asserting(_ shouldBe None)
        } yield Succeeded
      }
    }
  }

  private lazy val subscriberId = subscriberIds.generateOne
  private lazy val sourceUrl    = microserviceBaseUrls.generateOne

  private def finder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new LostSubscriberEventFinder[IO]
  }

  private def addEvent(status: EventStatus, project: Project)(implicit cfg: DBConfig[EventLogDB]) =
    storeGeneratedEvent(status, eventDates.generateOne, project)
}
