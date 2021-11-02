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

package io.renku.eventlog.subscriptions.zombieevents

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.events.consumers.subscriptions._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples}
import io.renku.graph.model.events.{CompoundEventId, EventStatus}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class LostSubscriberEventFinderSpec extends AnyWordSpec with IOSpec with InMemoryEventLogDbSpec with should.Matchers {

  "popEvent" should {

    List(GeneratingTriples, TransformingTriples) foreach { status =>
      "return event which belongs to a subscriber not listed in the subscriber table " +
        s"if event's status is $status" in new TestCase {

          addEvent(eventId, status)
          upsertEventDelivery(eventId, subscriberId)

          val notLostEvent        = compoundEventIds.generateOne
          val activeSubscriberId  = subscriberIds.generateOne
          val activeSubscriberUrl = subscriberUrls.generateOne
          addEvent(notLostEvent, status)
          upsertSubscriber(activeSubscriberId, activeSubscriberUrl, sourceUrl)
          upsertEventDelivery(notLostEvent, activeSubscriberId)

          finder.popEvent().unsafeRunSync() shouldBe ZombieEvent(finder.processName, eventId, projectPath, status).some
          finder.popEvent().unsafeRunSync() shouldBe None
        }
    }

    EventStatus.all.filterNot(status => status == GeneratingTriples || status == TransformingTriples) foreach {
      status =>
        s"return None when the event has a the status $status" in new TestCase {
          addEvent(eventId, status)
          upsertEventDelivery(eventId, subscriberId)

          finder.popEvent().unsafeRunSync() shouldBe None
        }
    }
  }

  private trait TestCase {
    val eventId          = compoundEventIds.generateOne
    val subscriberId     = subscriberIds.generateOne
    val sourceUrl        = microserviceBaseUrls.generateOne
    val projectPath      = projectPaths.generateOne
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")

    val finder = new LostSubscriberEventFinder(sessionResource, queriesExecTimes)

    def addEvent(eventId: CompoundEventId, status: EventStatus): Unit = storeEvent(
      eventId,
      status,
      executionDates.generateOne,
      eventDates.generateOne,
      eventBodies.generateOne,
      projectPath = projectPath
    )
  }
}
