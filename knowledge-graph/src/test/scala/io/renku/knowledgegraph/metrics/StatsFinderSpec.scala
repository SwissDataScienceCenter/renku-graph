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

package io.renku.knowledgegraph.metrics

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.toShow
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.Property
import io.renku.knowledgegraph.KnowledgeGraphJenaSpec
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StatsFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with KnowledgeGraphJenaSpec
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "entitiesCount" should {

    "return zero if there are no entity in the DB" in projectsDSConfig.use { implicit pcc =>
      stats.entitiesCount().asserting {
        _ shouldBe Map(
          EntityLabel((schema / "Dataset").show)              -> Count(0L),
          EntityLabel((schema / "Project").show)              -> Count(0L),
          EntityLabel((prov / "Activity").show)               -> Count(0L),
          EntityLabel((prov / "Plan").show)                   -> Count(0L),
          EntityLabel((schema / "Person").show)               -> Count(0L),
          EntityLabel((schema / "Person with GitLabId").show) -> Count(0L)
        )
      }
    }

    "return info about number of objects by types" in projectsDSConfig.use { implicit pcc =>
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

      uploadToProjects(projectsWithDatasets.map(_._2) ::: projectsWithActivities: _*) >>
        stats.entitiesCount().asserting(_ shouldBe entitiesWithActivities)
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def stats(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new StatsFinderImpl[IO](pcc)
  }

  private implicit class MapOps(entitiesByType: Map[EntityLabel, Count]) {
    def update(entityType: Property, count: Long): Map[EntityLabel, Count] = {
      val entity       = EntityLabel(entityType.show)
      val runningTotal = entitiesByType.getOrElse(entity, Count(0L)).value
      entitiesByType.updated(entity, Count(runningTotal + count))
    }
  }
}
