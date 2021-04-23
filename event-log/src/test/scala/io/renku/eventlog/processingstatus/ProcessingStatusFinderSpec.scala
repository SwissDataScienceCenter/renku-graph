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

package io.renku.eventlog.processingstatus

import cats.data.NonEmptyList
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{BatchDate, EventStatus}
import ch.datascience.graph.model.projects.Id
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.temporal.ChronoUnit._

class ProcessingStatusFinderSpec extends AnyWordSpec with InMemoryEventLogDbSpec with should.Matchers {

  "fetchStatus" should {

    "return ProcessingStatus for the given project " +
      s"where $TriplesStore, $Skipped, $GenerationNonRecoverableFailure, $TransformationNonRecoverableFailure events are counted as done " +
      "and all as total" in new TestCase {

        storeEvents(projectIds.generateOne, batchDates.generateOne, nonEmptyList(eventStatuses).generateOne)

        val toBeProcessedEvents = nonEmptyList(
          Gen.oneOf(New,
                    GeneratingTriples,
                    TriplesGenerated,
                    TransformingTriples,
                    GenerationRecoverableFailure,
                    TransformationRecoverableFailure
          ),
          minElements = 10,
          maxElements = 20
        ).generateOne
        val doneEvents = nonEmptyList(
          Gen.oneOf(TriplesStore, Skipped, GenerationNonRecoverableFailure, TransformationNonRecoverableFailure),
          minElements = 10,
          maxElements = 20
        ).generateOne
        val batchDate = batchDates.generateOne
        storeEvents(projectId, batchDate, toBeProcessedEvents ::: doneEvents)

        val Some(processingStatus) = processingStatusFinder.fetchStatus(projectId).value.unsafeRunSync()

        val expectedTotal = doneEvents.size + toBeProcessedEvents.size
        processingStatus.done.value           shouldBe doneEvents.size
        processingStatus.total.value          shouldBe expectedTotal
        processingStatus.progress.value.floor shouldBe ((doneEvents.size.toDouble / expectedTotal) * 100).floor

        queriesExecTimes.verifyExecutionTimeMeasured("processing status")
      }

    "return ProcessingStatus for the latest batch only" in new TestCase {

      val batch1Date     = batchDates.generateOne
      val batch1Statuses = nonEmptyList(eventStatuses).generateOne
      storeEvents(projectId, batch1Date, batch1Statuses)

      val batch2Date     = batchDates generateDifferentThan batch1Date
      val batch2Statuses = nonEmptyList(eventStatuses).generateOne
      storeEvents(projectId, batch2Date, batch2Statuses)

      val Some(processingStatus) = processingStatusFinder.fetchStatus(projectId).value.unsafeRunSync()

      val latestBatchStatuses =
        if ((batch1Date compareTo batch2Date) > 0) batch1Statuses
        else batch2Statuses
      processingStatus.total.value shouldBe latestBatchStatuses.size
    }

    "return ProcessingStatus with done=total=(events in the batch) " +
      "if all events from the latest batch are processed" in new TestCase {

        val olderBatchDate     = batchDates.generateOne
        val olderBatchStatuses = nonEmptyList(eventStatuses).generateOne
        storeEvents(projectId, olderBatchDate, olderBatchStatuses)

        val newerBatchDate = BatchDate(olderBatchDate.value.plus(1, MINUTES))
        val newerBatchStatuses =
          nonEmptyList(
            Gen.oneOf(TriplesStore, Skipped, GenerationNonRecoverableFailure, TransformationNonRecoverableFailure)
          ).generateOne
        storeEvents(projectId, newerBatchDate, newerBatchStatuses)

        val Some(processingStatus) = processingStatusFinder.fetchStatus(projectId).value.unsafeRunSync()

        processingStatus.total.value    shouldBe newerBatchStatuses.size
        processingStatus.done.value     shouldBe newerBatchStatuses.size
        processingStatus.progress.value shouldBe 100d
      }

    "return None if there are no events for the project id" in new TestCase {
      processingStatusFinder.fetchStatus(projectId).value.unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    val projectId              = projectIds.generateOne
    val queriesExecTimes       = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val processingStatusFinder = new ProcessingStatusFinderImpl(sessionResource, queriesExecTimes)

    def storeEvents(projectId: Id, batchDate: BatchDate, statuses: NonEmptyList[EventStatus]) =
      statuses map {
        storeEvent(
          compoundEventIds.generateOne.copy(projectId = projectId),
          _,
          executionDates.generateOne,
          eventDates.generateOne,
          eventBodies.generateOne,
          createdDates.generateOne,
          batchDate
        )
      }
  }
}
