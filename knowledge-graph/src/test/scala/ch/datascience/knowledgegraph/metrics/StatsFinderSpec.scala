/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import ch.datascience.generators.Generators.{listOf, nonBlankStrings, nonEmptyList, nonEmptyStrings, setOf}
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.datasets
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators.datasetProjects
import ch.datascience.knowledgegraph.datasets.model.{Dataset, DatasetProject}
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.Person.persons
import ch.datascience.rdfstore.entities.RunPlan.Command
import ch.datascience.rdfstore.entities.bundles.{generateProject, renkuBaseUrl}
import ch.datascience.rdfstore.entities.{Activity, Agent, Association, DataSet, Generation, ProcessRun, RunPlan, WorkflowFile, WorkflowRun}
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StatsFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {
  "entitiesCount" should {
    "return zero if there are no entity in the DB" in new TestCase {
      stats.entitiesCount.unsafeRunSync() shouldBe KGEntityType.all.map(entityType => entityType -> 0).toMap
    }

    "return info about number of objects by types" in new TestCase {
      implicit class MapOps(entitiesByType: Map[KGEntityType, List[JsonLD]]) {
        def update(entityType: KGEntityType, entities: List[JsonLD]) =
          entitiesByType
            .updated(entityType, entitiesByType.getOrElse(entityType, List.empty[JsonLD]) ++: entities)
      }

      val projects = nonEmptyList(datasetProjects).generateOne
      val entitiesToStore = projects.toList.foldLeft(Map.empty[KGEntityType, List[JsonLD]]) {
        case (entitiesByType, project) =>
          val commitActivities = listOf(datasetsWithActivity(project)).generateOne
          val processRuns      = listOf(processRunsJsonLDs(project)).generateOne
          val workflowRuns = commitActivities.headOption match {
            case Some(activity) =>
              listOf(
                workflowJsonLDs(project, activity)
              ).generateOne
            case None => List.empty[JsonLD]
          }

          entitiesByType
            .update(KGEntityType.Dataset, commitActivities.map(_.asJsonLD))
            .update(KGEntityType.ProcessRun, processRuns)
            .update(KGEntityType.WorkflowRun, workflowRuns)
      }

      loadToStore(entitiesToStore.values.flatten.toList: _*)

      private val numberOfDatasetCommits:  Long = entitiesToStore(KGEntityType.Dataset).size
      private val numberOfWorkflowRuns:    Long = entitiesToStore(KGEntityType.WorkflowRun).size
      private val numberOfProcessRuns:     Long = entitiesToStore(KGEntityType.ProcessRun).size + numberOfWorkflowRuns
      private val totalNumberOfActivities: Long = numberOfDatasetCommits + numberOfProcessRuns

      stats.entitiesCount.unsafeRunSync() shouldBe Map[KGEntityType, Long](
        KGEntityType.Dataset     -> numberOfDatasetCommits,
        KGEntityType.Project     -> projects.size.toLong,
        KGEntityType.ProcessRun  -> numberOfProcessRuns,
        KGEntityType.WorkflowRun -> numberOfWorkflowRuns,
        KGEntityType.Activity    -> totalNumberOfActivities
      )
    }

  }

  trait TestCase {
    private val logger        = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val stats =
      new StatsFinderImpl(rdfStoreConfig, logger, new SparqlQueryTimeRecorder(executionTimeRecorder))
  }

  import eu.timepit.refined.auto._

  private def datasetsWithActivity(datasetProjects: DatasetProject): Gen[Activity] =
    for {
      dataset <- DatasetsGenerators.datasets
    } yield toDataSetCommit(dataset.copy(projects = List(datasetProjects)))

  private def toDataSetCommit(dataSet: Dataset): Activity =
    dataSet.projects match {
      case datasetProject +: Nil =>
        val project       = generateProject(datasetProject.path)
        val committedDate = committedDates.generateOne
        Activity(
          commitIds.generateOne,
          committedDate,
          persons.generateOne,
          project,
          Agent(cliVersions.generateOne),
          maybeGenerationFactories = List(
            Generation.factory(
              DataSet.factory(
                dataSet.id,
                dataSet.title,
                dataSet.name,
                dataSet.maybeUrl,
                createdDate    = datasets.DateCreated(committedDate.value),
                creators       = setOf(persons).generateOne,
                partsFactories = List()
              )
            )
          )
        )
      case _ => fail("Not prepared to work datasets having multiple projects")
    }

  private def processRunsJsonLDs(datasetProject: DatasetProject): Gen[JsonLD] =
    for {
      commitId   <- commitIds
      commitDate <- committedDates
      committer  <- persons
      cliVersion <- cliVersions
      project = generateProject(datasetProject.path)
      agent   = Agent(cliVersion)
      comment      <- nonEmptyStrings()
      workflowFile <- nonBlankStrings()
    } yield ProcessRun
      .standAlone(
        commitId,
        commitDate,
        committer,
        project,
        agent,
        comment,
        None,
        Association.process(
          agent.copy(cliVersion = cliVersions.generateOne),
          RunPlan.process(
            WorkflowFile.yaml(workflowFile),
            Command("python"),
            inputs  = List(),
            outputs = List()
          )
        )
      )
      .asJsonLD

  private def workflowJsonLDs(datasetProject: DatasetProject, activity: Activity) =
    for {
      commitId   <- commitIds
      commitDate <- committedDates
      committer  <- persons
      cliVersion <- cliVersions
      project = generateProject(datasetProject.path)
      agent   = Agent(cliVersion)
      comment <- nonEmptyStrings()
    } yield WorkflowRun(
      commitId,
      commitDate,
      committer,
      project,
      agent,
      comment = comment,
      WorkflowFile.yaml("renku-update.yaml"),
      informedBy = activity,
      associationFactory = Association.workflow(
        agent.copy(cliVersion = cliVersions.generateOne),
        RunPlan.workflow(
          inputs       = List(),
          outputs      = List(),
          subprocesses = List()
        )
      ),
      processRunsFactories = List(),
      maybeInvalidation    = None
    ).asJsonLD

}
