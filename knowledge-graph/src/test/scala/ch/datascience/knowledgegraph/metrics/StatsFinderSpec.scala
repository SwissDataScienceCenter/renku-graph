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
import ch.datascience.generators.CommonGraphGenerators.cliVersions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{nonBlankStrings, nonEmptyList, nonEmptyStrings}
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.EntitiesGenerators.persons
import ch.datascience.rdfstore.entities.RunPlan.Command
import ch.datascience.rdfstore.entities._
import ch.datascience.rdfstore.entities.bundles.{generateProject, gitLabApiUrl, nonModifiedDataSetCommit, renkuBaseUrl}
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import eu.timepit.refined.auto._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLD, Property}
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StatsFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "entitiesCount" should {

    "return zero if there are no entity in the DB" in new TestCase {
      stats.entitiesCount().unsafeRunSync() shouldBe Map(
        EntityType((schema / "Dataset").toString)              -> EntitiesCount(0L),
        EntityType((schema / "Project").toString)              -> EntitiesCount(0L),
        EntityType((prov / "Activity").toString)               -> EntitiesCount(0L),
        EntityType((wfprov / "ProcessRun").toString)           -> EntitiesCount(0L),
        EntityType((wfprov / "WorkflowRun").toString)          -> EntitiesCount(0L),
        EntityType((renku / "Run").toString)                   -> EntitiesCount(0L),
        EntityType((schema / "Person").toString)               -> EntitiesCount(0L),
        EntityType((schema / "Person with GitLabId").toString) -> EntitiesCount(0L)
      )
    }

    "return info about number of objects by types" in new TestCase {

      val entitiesByType = Map.empty[EntityType, EntitiesCount]

      val datasetsJsons = datasetsWithActivities.generateNonEmptyList().toList
      val entitiesWithDatasets = entitiesByType
        .update(schema / "Project", datasetsJsons.size)
        .update(prov / "Activity", datasetsJsons.size)
        .update(schema / "Dataset", datasetsJsons.size)
        .update(schema / "Person", datasetsJsons.size)
        .update(schema / "Person with GitLabId", 0)

      val processRunsJsons = nonEmptyList(processRuns).generateOne.toList
      val entitiesWithProcessRuns = entitiesWithDatasets
        .update(schema / "Project", processRunsJsons.size)
        .update(prov / "Activity", processRunsJsons.size)
        .update(wfprov / "ProcessRun", processRunsJsons.size)
        .update(renku / "Run", processRunsJsons.size)
        .update(schema / "Person", processRunsJsons.size)
        .update(schema / "Person with GitLabId", 0)

      val workflowsJsons = nonEmptyList(workflows).generateOne.toList
      val entitiesWithWorkflows = entitiesWithProcessRuns
        .update(schema / "Project", workflowsJsons.size)
        .update(prov / "Activity", workflowsJsons.size)
        .update(wfprov / "ProcessRun", workflowsJsons.size)
        .update(wfprov / "WorkflowRun", workflowsJsons.size)
        .update(renku / "Run", workflowsJsons.size)
        .update(schema / "Person", workflowsJsons.size)
        .update(schema / "Person with GitLabId", 0)

      loadToStore(datasetsJsons ++ processRunsJsons ++ workflowsJsons: _*)

      stats.entitiesCount().unsafeRunSync() shouldBe entitiesWithWorkflows
    }

    "return info about number of Person objects with GitLabId" in new TestCase {

      val entitiesByType = Map.empty[EntityType, EntitiesCount]

      val activity1Committer = persons.generateOne
      val activity1          = activityEntities.generateOne.copy(committer = activity1Committer)
      val activity2Committer = persons.generateOne.copy(maybeGitLabId = userGitLabIds.generateSome)
      val activity2          = activityEntities.generateOne.copy(committer = activity2Committer)

      loadToStore(activity1.asJsonLD, activity2.asJsonLD)

      stats.entitiesCount().unsafeRunSync() shouldBe entitiesByType
        .update(schema / "Dataset", 0)
        .update(schema / "Project", 2)
        .update(prov / "Activity", 2)
        .update(wfprov / "ProcessRun", 0)
        .update(wfprov / "WorkflowRun", 0)
        .update(renku / "Run", 0)
        .update(schema / "Person", 2)
        .update(schema / "Person with GitLabId", 1)
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

  private lazy val datasetsWithActivities: Gen[JsonLD] =
    for {
      datasetId   <- datasetIdentifiers
      projectPath <- projectPaths
      projectName <- projectNames
    } yield nonModifiedDataSetCommit()(projectPath, projectName)(datasetId)

  private lazy val processRuns: Gen[JsonLD] =
    for {
      projectPath <- projectPaths
      commitId    <- commitIds
      commitDate  <- committedDates
      committer   <- persons
      cliVersion  <- cliVersions
      agent = Agent(cliVersion)
      comment      <- nonEmptyStrings()
      workflowFile <- nonBlankStrings()
    } yield ProcessRun
      .standAlone(
        commitId,
        commitDate,
        committer,
        generateProject(projectPath),
        agent,
        comment,
        None,
        Association.process(
          agent.copy(cliVersion = cliVersions.generateOne),
          RunPlan.process(
            WorkflowFile.yaml(workflowFile),
            Command("python"),
            inputs = List(),
            outputs = List()
          )
        )
      )
      .asJsonLD

  private lazy val workflows =
    for {
      projectPath          <- projectPaths
      workflowCommitId     <- commitIds
      workflowCommitDate   <- committedDates
      informedByCommitId   <- commitIds
      informedByCommitDate <- committedDates
      committer            <- persons
      cliVersion           <- cliVersions
      agent = Agent(cliVersion)
      comment <- nonEmptyStrings()
    } yield {
      val project = generateProject(projectPath)
      WorkflowRun(
        workflowCommitId,
        workflowCommitDate,
        committer,
        project,
        agent,
        comment = comment,
        WorkflowFile.yaml("renku-update.yaml"),
        informedBy = Activity(informedByCommitId, informedByCommitDate, committer, project, Agent(cliVersion)),
        associationFactory = Association.workflow(
          agent.copy(cliVersion = cliVersions.generateOne),
          RunPlan.workflow(
            inputs = List(),
            outputs = List(),
            subprocesses = List()
          )
        ),
        processRunsFactories = List()
      ).asJsonLD
    }

  private implicit class MapOps(entitiesByType: Map[EntityType, EntitiesCount]) {
    def update(entityType: Property, count: Long): Map[EntityType, EntitiesCount] = {
      val entity       = EntityType(entityType.toString)
      val runningTotal = entitiesByType.getOrElse(entity, EntitiesCount(0L)).value
      entitiesByType.updated(entity, EntitiesCount(runningTotal + count))
    }
  }
}
