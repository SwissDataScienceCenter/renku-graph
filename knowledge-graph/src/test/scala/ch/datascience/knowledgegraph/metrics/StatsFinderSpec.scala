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

package ch.datascience.knowledgegraph.metrics

import cats.effect.IO
import cats.implicits.toShow
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.cliVersions
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.testentities.EntitiesGenerators.personEntities
import ch.datascience.graph.model.testentities._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import eu.timepit.refined.auto._
import io.renku.jsonld.Property
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StatsFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "entitiesCount" should {

    "return zero if there are no entity in the DB" in new TestCase {
      stats.entitiesCount().unsafeRunSync() shouldBe Map(
        EntityLabel((schema / "Dataset").show)              -> Count(0L),
        EntityLabel((schema / "Project").show)              -> Count(0L),
        EntityLabel((prov / "Activity").show)               -> Count(0L),
        EntityLabel((renku / "Run").show)                   -> Count(0L),
        EntityLabel((schema / "Person").show)               -> Count(0L),
        EntityLabel((schema / "Person with GitLabId").show) -> Count(0L)
      )
    }

    "return info about number of objects by types" in new TestCase {

      val datasets   = datasetEntities(ofAnyProvenance).generateNonEmptyList()
      val activities = activityEntities.generateNonEmptyList(minElements = 10, maxElements = 50)
      val persons    = activities.map(_.author)

      val entitiesWithActivities = Map
        .empty[EntityLabel, Count]
        .update(schema / "Dataset", datasets.size)
        .update(schema / "Project", activities.size + datasets.size)
        .update(prov / "Activity", activities.size)
        .update(renku / "Run", activities.map(_.association.runPlan).size)
        .update(schema / "Person", persons.size)
        .update(schema / "Person with GitLabId", persons.toList.count(_.maybeGitLabId.isDefined))

      loadToStore(
        datasets.toList.map(_.asJsonLD) :::
          activities.toList.map(_.asJsonLD) :::
          activities.toList.map(_.association.runPlan.asJsonLD): _*
      )

      stats.entitiesCount().unsafeRunSync() shouldBe entitiesWithActivities
    }
  }

  private trait TestCase {
    private val logger = TestLogger[IO]()
    val stats = new StatsFinderImpl(
      rdfStoreConfig,
      logger,
      new SparqlQueryTimeRecorder(TestExecutionTimeRecorder[IO](logger))
    )
  }

  private lazy val activityEntities: Gen[Activity] = for {
    name       <- runPlanNames
    command    <- runPlanCommands
    author     <- personEntities
    cliVersion <- cliVersions
    project    <- projectEntities[ForksCount.Zero](visibilityPublic)
    startTime  <- activityStartTimes(project.dateCreated)
  } yield ExecutionPlanner
    .of(RunPlan(name, command, commandParameterFactories = Nil, project), startTime, author, cliVersion)
    .buildProvenanceGraph
    .fold(errors => fail(errors.intercalate("\n")), identity)

  private implicit class MapOps(entitiesByType: Map[EntityLabel, Count]) {
    def update(entityType: Property, count: Long): Map[EntityLabel, Count] = {
      val entity       = EntityLabel(entityType.show)
      val runningTotal = entitiesByType.getOrElse(entity, Count(0L)).value
      entitiesByType.updated(entity, Count(runningTotal + count))
    }
  }
}
