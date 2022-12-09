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

package io.renku.knowledgegraph.metrics

import cats.effect.IO
import cats.implicits.toShow
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.Property
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StatsFinderSpec
    extends AnyWordSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with ScalaCheckPropertyChecks
    with should.Matchers
    with IOSpec {

  "entitiesCount" should {

    "return zero if there are no entity in the DB" in new TestCase {
      stats.entitiesCount().unsafeRunSync() shouldBe Map(
        EntityLabel((schema / "Dataset").show)              -> Count(0L),
        EntityLabel((schema / "Project").show)              -> Count(0L),
        EntityLabel((prov / "Activity").show)               -> Count(0L),
        EntityLabel((prov / "Plan").show)                   -> Count(0L),
        EntityLabel((schema / "Person").show)               -> Count(0L),
        EntityLabel((schema / "Person with GitLabId").show) -> Count(0L)
      )
    }

    "return info about number of objects by types" in new TestCase {

      val projectsWithDatasets =
        anyRenkuProjectEntities.addDataset(datasetEntities(provenanceNonModified)).generateNonEmptyList().toList
      val projectsWithActivities = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .generateNonEmptyList(min = 10, max = 50)
        .toList
      val persons = projectsWithActivities.flatMap(_.activities.map(_.author))

      val entitiesWithActivities = Map
        .empty[EntityLabel, Count]
        .update(schema / "Dataset", projectsWithDatasets.size)
        .update(schema / "Project", projectsWithActivities.size + projectsWithDatasets.size)
        .update(prov / "Activity", projectsWithActivities.size)
        .update(prov / "Plan", projectsWithActivities.size)
        .update(schema / "Person", persons.size)
        .update(schema / "Person with GitLabId", persons.count(_.maybeGitLabId.isDefined))

      upload(to = projectsDataset, projectsWithDatasets.map(_._2) ::: projectsWithActivities: _*)

      stats.entitiesCount().unsafeRunSync() shouldBe entitiesWithActivities
    }
  }

  private trait TestCase {
    implicit val logger:               TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val stats = new StatsFinderImpl[IO](projectsDSConnectionInfo)
  }

  private implicit class MapOps(entitiesByType: Map[EntityLabel, Count]) {
    def update(entityType: Property, count: Long): Map[EntityLabel, Count] = {
      val entity       = EntityLabel(entityType.show)
      val runningTotal = entitiesByType.getOrElse(entity, Count(0L)).value
      entitiesByType.updated(entity, Count(runningTotal + count))
    }
  }
}
