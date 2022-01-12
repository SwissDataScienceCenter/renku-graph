/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonNegativeInts
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects
import io.renku.metrics.LabeledGauge
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class GaugesUpdaterSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "updateGauges" should {

    s"update values for all projects in all the gauges in case of the ${DBUpdateResults.ForProjects} update" in new TestCase {
      val projectsPath = projectPaths.generateNonEmptyList()

      val updateResults =
        DBUpdateResults.ForProjects(projectsPath.map(_ -> countsForAllStatuses.generateOne).toList.toSet)

      projectsPath.toList.foreach { projectPath =>
        val awaitingGenerationChange = List(
          updateResults.getCount(projectPath, New),
          updateResults.getCount(projectPath, GenerationRecoverableFailure)
        ).sum.toDouble
        (awaitingGenerationGauge.update _).expects(projectPath -> awaitingGenerationChange).returning(().pure[Try])

        val awaitingTransformationChange = List(
          updateResults.getCount(projectPath, TriplesGenerated),
          updateResults.getCount(projectPath, TransformationRecoverableFailure)
        ).sum.toDouble
        (awaitingTransformationGauge.update _)
          .expects(projectPath -> awaitingTransformationChange)
          .returning(().pure[Try])

        val underGenerationGaugeChange = List(updateResults.getCount(projectPath, GeneratingTriples)).sum.toDouble
        (underTriplesGenerationGauge.update _)
          .expects(projectPath -> underGenerationGaugeChange)
          .returning(().pure[Try])

        val underTransformationGaugeChange = List(updateResults.getCount(projectPath, TransformingTriples)).sum.toDouble
        (underTransformationGauge.update _)
          .expects(projectPath -> underTransformationGaugeChange)
          .returning(().pure[Try])
      }

      gaugesUpdater.updateGauges(updateResults) shouldBe ().pure[Try]
    }

    s"reset all the gauges in case of ${DBUpdateResults.ForAllProjects} update" in new TestCase {

      val updateResults = DBUpdateResults.ForAllProjects

      (awaitingGenerationGauge.reset _).expects().returning(().pure[Try])
      (awaitingTransformationGauge.reset _).expects().returning(().pure[Try])
      (underTriplesGenerationGauge.reset _).expects().returning(().pure[Try])
      (underTransformationGauge.reset _).expects().returning(().pure[Try])

      gaugesUpdater.updateGauges(updateResults) shouldBe ().pure[Try]
    }
  }

  private trait TestCase {

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

  private implicit class DBUpdateResultsOps(dbUpdateResults: DBUpdateResults.ForProjects) {
    def getCount(projectPath: projects.Path, eventStatus: EventStatus): Int =
      dbUpdateResults.statusCounts
        .find { case (path, _) => path == projectPath }
        .map { case (_, statusCount) =>
          statusCount.getOrElse(eventStatus, 0)
        }
        .getOrElse(0)
  }
}
