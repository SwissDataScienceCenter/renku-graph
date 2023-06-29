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

package io.renku.eventlog.events.consumers.projectsync

import cats.effect.IO
import cats.syntax.all._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.RenkuTinyTypeGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.graph.model.{events, projects}
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.CustomAsyncIOSpec
import org.scalacheck.Gen
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class PayloadFinderSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with OptionValues
    with InMemoryEventLogDbSpec {

  it should "return the latest event payload for the project when one exists " +
    "- case when the last event has a payload" in {

      val projectId = projectIds.generateOne
      val payload   = addEvent(projectId, Gen.oneOf(payloadStatues).generateOne).value

      finder.findLatestPayload(projectId).asserting(_.value.value should contain theSameElementsAs payload.value.value)
    }

  it should "return the latest event payload for the project when one exists " +
    "- case there are many" in {

      val projectId = projectIds.generateOne

      val oldestEventDate = eventDates.generateOne
      val payload         = addEvent(projectId, Gen.oneOf(payloadStatues).generateOne, oldestEventDate).value

      addEvent(projectId,
               Gen.oneOf(payloadStatues).generateOne,
               timestamps(max = oldestEventDate.value.minusSeconds(1)).generateAs(events.EventDate)
      )

      finder.findLatestPayload(projectId).asserting(_.value.value should contain theSameElementsAs payload.value.value)
    }

  it should "return the latest event with payload for the project " +
    "- case where the latest project event does not have a payload" in {

      val projectId = projectIds.generateOne

      val oldestEventDate = eventDates.generateOne
      addEvent(projectId, Gen.oneOf(nonPayloadStatuses).generateOne, oldestEventDate)

      val payload = addEvent(projectId,
                             Gen.oneOf(payloadStatues).generateOne,
                             timestamps(max = oldestEventDate.value.minusSeconds(1)).generateAs(events.EventDate)
      ).value

      finder.findLatestPayload(projectId).asserting(_.value.value should contain theSameElementsAs payload.value.value)
    }

  it should "return no payload when there's no event with payload for the project" in {

    val projectId = projectIds.generateOne

    addEvent(projectId, Gen.oneOf(nonPayloadStatuses).generateOne).pure[IO].asserting(_ shouldBe None) >>
      finder.findLatestPayload(projectId).asserting(_ shouldBe None)
  }

  private implicit lazy val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
  private implicit lazy val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
  private lazy val finder = new PayloadFinderImpl[IO]

  private def addEvent(projectId: projects.GitLabId,
                       status:    events.EventStatus,
                       eventDate: events.EventDate = eventDates.generateOne
  ): Option[ZippedEventPayload] = {
    val eventId = compoundEventIds.generateOne.copy(projectId = projectId)
    val maybeEventPayload = status match {
      case s if payloadStatues contains s => zippedEventPayloads.generateSome
      case _                              => zippedEventPayloads.generateNone
    }
    storeEvent(
      eventId,
      status,
      timestamps.generateAs(events.ExecutionDate),
      eventDate,
      eventBodies.generateOne,
      maybeMessage = status match {
        case _: FailureStatus => eventMessages.generateSome
        case _ => eventMessages.generateOption
      },
      maybeEventPayload = maybeEventPayload,
      projectPath = projectPaths.generateOne
    )
    maybeEventPayload
  }

  private lazy val payloadStatues = Set(TriplesGenerated,
                                        TransformingTriples,
                                        TransformationRecoverableFailure,
                                        TransformationNonRecoverableFailure,
                                        TriplesStore
  )

  private lazy val nonPayloadStatuses = events.EventStatus.all -- payloadStatues
}
