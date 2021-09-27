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

package io.renku.eventlog.events

import Generators._
import cats.effect.IO
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventsFinderSpec extends AnyWordSpec with InMemoryEventLogDbSpec with should.Matchers {

  "findEvents" should {

    "return the List of events of the project the given path" in new TestCase {
      val projectId     = projectIds.generateOne
      val infosAndDates = eventInfos.generateList(maxElements = 10).map(_ -> eventDates.generateOne)

      infosAndDates foreach { case (info, eventDate) =>
        val eventId = CompoundEventId(info.eventId, projectId)
        storeEvent(
          eventId,
          info.status,
          executionDates.generateOne,
          eventDate,
          eventBodies.generateOne,
          projectPath = projectPath,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }
      storeGeneratedEvent(
        eventStatuses.generateOne,
        eventDates.generateOne,
        projectIds.generateOne,
        projectPaths.generateOne
      )

      eventsFinder.findEvents(projectPath).unsafeRunSync() shouldBe infosAndDates.sortBy(_._2).map(_._1).reverse
    }

    "return an empty List if there's no project with the given path" in new TestCase {
      eventsFinder.findEvents(projectPath).unsafeRunSync() shouldBe Nil
    }

    "return an empty List if there are no events for the project with the given path" in new TestCase {
      upsertProject(projectIds.generateOne, projectPaths.generateOne, eventDates.generateOne)
      eventsFinder.findEvents(projectPath).unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val eventsFinder     = new EventsFinderImpl[IO](sessionResource, queriesExecTimes)
  }
}
