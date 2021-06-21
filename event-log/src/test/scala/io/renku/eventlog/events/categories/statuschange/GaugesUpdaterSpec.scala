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

package io.renku.eventlog.events.categories.statuschange

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonNegativeInts
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class GaugesUpdaterSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "updateGauges" should {

    "update values in all the gauges depending on the given update values for a specific project" in new TestCase {

      val updateResults = DBUpdateResults.ForProject(projectPath, countsForAllStatuses.generateOne)

      val awaitingGenerationChange = List(
        updateResults.getCount(New),
        updateResults.getCount(GenerationRecoverableFailure)
      ).sum.toDouble
      (awaitingGenerationGauge.update _).expects(projectPath -> -awaitingGenerationChange).returning(().pure[Try])

      val awaitingTransformationChange = List(
        updateResults.getCount(TriplesGenerated),
        updateResults.getCount(TransformationRecoverableFailure)
      ).sum.toDouble
      (awaitingTransformationGauge.update _)
        .expects(projectPath -> -awaitingTransformationChange)
        .returning(().pure[Try])

      val underGenerationGaugeChange = List(updateResults.getCount(GeneratingTriples)).sum.toDouble
      (underTriplesGenerationGauge.update _)
        .expects(projectPath -> -underGenerationGaugeChange)
        .returning(().pure[Try])

      val underTransformationGaugeChange = List(updateResults.getCount(TransformingTriples)).sum.toDouble
      (underTransformationGauge.update _)
        .expects(projectPath -> -underTransformationGaugeChange)
        .returning(().pure[Try])

      gaugesUpdater.updateGauges(updateResults) shouldBe ().pure[Try]
    }

    "reset values in all the gauges for all projects" in new TestCase {

      val updateResults = DBUpdateResults.ForAllProjects

      (awaitingGenerationGauge.reset _).expects().returning(().pure[Try])

      (awaitingTransformationGauge.reset _).expects().returning(().pure[Try])

      (underTriplesGenerationGauge.reset _).expects().returning(().pure[Try])

      (underTransformationGauge.reset _).expects().returning(().pure[Try])

      gaugesUpdater.updateGauges(updateResults) shouldBe ().pure[Try]
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    val awaitingGenerationGauge     = mock[LabeledGauge[Try, projects.Path]]
    val awaitingTransformationGauge = mock[LabeledGauge[Try, projects.Path]]
    val underTransformationGauge    = mock[LabeledGauge[Try, projects.Path]]
    val underTriplesGenerationGauge = mock[LabeledGauge[Try, projects.Path]]
    val gaugesUpdater = new GaugesUpdaterImpl[Try](awaitingGenerationGauge,
                                                   awaitingTransformationGauge,
                                                   underTransformationGauge,
                                                   underTriplesGenerationGauge
    )
  }

  private def countsForAllStatuses: Gen[Map[EventStatus, Int]] = for {
    statusNew                           <- nonNegativeInts()
    generatingTriples                   <- nonNegativeInts()
    triplesGenerated                    <- nonNegativeInts()
    transformingTriples                 <- nonNegativeInts()
    triplesStore                        <- nonNegativeInts()
    skipped                             <- nonNegativeInts()
    generationRecoverableFailure        <- nonNegativeInts()
    generationNonRecoverableFailure     <- nonNegativeInts()
    transformationRecoverableFailure    <- nonNegativeInts()
    transformationNonRecoverableFailure <- nonNegativeInts()
    awaitingDeletion                    <- nonNegativeInts()
  } yield Map(
    New                                 -> statusNew.value,
    GeneratingTriples                   -> generatingTriples.value,
    TriplesGenerated                    -> triplesGenerated.value,
    TransformingTriples                 -> transformingTriples.value,
    TriplesStore                        -> triplesStore.value,
    Skipped                             -> skipped.value,
    GenerationRecoverableFailure        -> generationRecoverableFailure.value,
    GenerationNonRecoverableFailure     -> generationNonRecoverableFailure.value,
    TransformationRecoverableFailure    -> transformationRecoverableFailure.value,
    TransformationNonRecoverableFailure -> transformationNonRecoverableFailure.value,
    AwaitingDeletion                    -> awaitingDeletion.value
  )

  private implicit class DBUpdateResultsOps(dbUpdateResults: DBUpdateResults.ForProject) {
    def getCount(eventStatus: EventStatus): Int = dbUpdateResults.changedStatusCounts.getOrElse(eventStatus, 0)
  }
}
