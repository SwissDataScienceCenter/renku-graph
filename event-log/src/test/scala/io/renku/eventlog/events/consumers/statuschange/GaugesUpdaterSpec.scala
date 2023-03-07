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

package io.renku.eventlog.events.consumers.statuschange

import cats.effect.IO
import io.renku.eventlog.metrics.{EventStatusGauges, TestEventStatusGauges}
import io.renku.eventlog.metrics.TestEventStatusGauges._
import io.renku.generators.Generators.{nonNegativeDoubles, nonNegativeInts}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GaugesUpdaterSpec extends AnyWordSpec with should.Matchers with MockFactory with IOSpec {

  "updateGauges" should {

    s"update values for all projects in all the gauges in case of the ${DBUpdateResults.ForProjects} update" in new TestCase {
      val projectsPath = projectPaths.generateNonEmptyList()

      val updateResults =
        DBUpdateResults.ForProjects(projectsPath.map(_ -> countsForAllStatuses.generateOne).toList.toSet)

      gaugesUpdater.updateGauges(updateResults).unsafeRunSync() shouldBe ()

      projectsPath.toList.foreach { projectPath =>
        val awaitingGenerationChange = List(
          updateResults.getCount(projectPath, New),
          updateResults.getCount(projectPath, GenerationRecoverableFailure)
        ).sum.toDouble
        gauges.awaitingGeneration.getValue(projectPath).unsafeRunSync() shouldBe awaitingGenerationChange

        val awaitingTransformationChange = List(
          updateResults.getCount(projectPath, TriplesGenerated),
          updateResults.getCount(projectPath, TransformationRecoverableFailure)
        ).sum.toDouble
        gauges.awaitingTransformation.getValue(projectPath).unsafeRunSync() shouldBe awaitingTransformationChange

        val underGenerationGaugeChange = List(updateResults.getCount(projectPath, GeneratingTriples)).sum.toDouble
        gauges.underGeneration.getValue(projectPath).unsafeRunSync() shouldBe underGenerationGaugeChange

        val underTransformationGaugeChange = List(updateResults.getCount(projectPath, TransformingTriples)).sum.toDouble
        gauges.underTransformation.getValue(projectPath).unsafeRunSync() shouldBe underTransformationGaugeChange

        val awaitingDeletionGaugeChange = List(updateResults.getCount(projectPath, AwaitingDeletion)).sum.toDouble
        gauges.awaitingDeletion.getValue(projectPath).unsafeRunSync() shouldBe awaitingDeletionGaugeChange

        val underDeletingGaugeChange = List(updateResults.getCount(projectPath, Deleting)).sum.toDouble
        gauges.underDeletion.getValue(projectPath).unsafeRunSync() shouldBe underDeletingGaugeChange
      }
    }

    s"reset all the gauges in case of ${DBUpdateResults.ForAllProjects} update" in new TestCase {

      val updateResults = DBUpdateResults.ForAllProjects

      gauges.awaitingGeneration
        .set(projectPaths.generateOne -> nonNegativeDoubles().generateOne.value)
        .unsafeRunSync()
      gauges.underGeneration
        .set(projectPaths.generateOne -> nonNegativeDoubles().generateOne.value)
        .unsafeRunSync()
      gauges.awaitingTransformation
        .set(projectPaths.generateOne -> nonNegativeDoubles().generateOne.value)
        .unsafeRunSync()
      gauges.underTransformation
        .set(projectPaths.generateOne -> nonNegativeDoubles().generateOne.value)
        .unsafeRunSync()
      gauges.awaitingDeletion
        .set(projectPaths.generateOne -> nonNegativeDoubles().generateOne.value)
        .unsafeRunSync()
      gauges.underDeletion
        .set(projectPaths.generateOne -> nonNegativeDoubles().generateOne.value)
        .unsafeRunSync()

      gaugesUpdater.updateGauges(updateResults).unsafeRunSync() shouldBe ()

      gauges.awaitingGeneration.getAllValues.unsafeRunSync()     shouldBe Map.empty
      gauges.underGeneration.getAllValues.unsafeRunSync()        shouldBe Map.empty
      gauges.awaitingTransformation.getAllValues.unsafeRunSync() shouldBe Map.empty
      gauges.underTransformation.getAllValues.unsafeRunSync()    shouldBe Map.empty
      gauges.awaitingDeletion.getAllValues.unsafeRunSync()       shouldBe Map.empty
      gauges.underDeletion.getAllValues.unsafeRunSync()          shouldBe Map.empty
    }
  }

  private trait TestCase {

    implicit val gauges: EventStatusGauges[IO] = TestEventStatusGauges[IO]
    val gaugesUpdater = new GaugesUpdaterImpl[IO]
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
    deleting                            <- nonNegativeInts()
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
    AwaitingDeletion                    -> awaitingDeletion.value,
    Deleting                            -> deleting.value
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
