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
import ch.datascience.db.SqlQuery
import ch.datascience.generators.CommonGraphGenerators.microserviceBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples}
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.subscriptions.Generators.{subscriberIds, subscriberUrls}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class LostSubscriberEventFinderSpec extends AnyWordSpec with InMemoryEventLogDbSpec with should.Matchers {

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

          finder.popEvent().unsafeRunSync() shouldBe ZombieEvent(eventId, projectPath, status).some
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
    val queriesExecTimes = TestLabeledHistogram[SqlQuery.Name]("query_id")

    val finder = new LostSubscriberEventFinder(transactor, queriesExecTimes)

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
