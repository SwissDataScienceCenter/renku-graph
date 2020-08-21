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
import ch.datascience.generators.Generators.{listOf, nonBlankStrings, nonEmptyList, nonEmptyStrings}
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.datasets.DateCreated
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators.{datasetProjects, datasets}
import ch.datascience.knowledgegraph.datasets.model.{Dataset, DatasetProject}
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.Person.persons
import ch.datascience.rdfstore.entities.RunPlan.Command
import ch.datascience.rdfstore.entities.bundles.{dataSetCommit, generateProject, renkuBaseUrl}
import ch.datascience.rdfstore.entities.{Agent, Association, Person, ProcessRun, RunPlan, WorkflowFile}
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.jsonld.JsonLD
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
      val projects = nonEmptyList(datasetProjects).generateOne
      val (datasets, processRuns) = projects.toList.foldLeft((List.empty[JsonLD], List.empty[JsonLD])) {
        case ((datasetsAcc, processRunsAcc), project) =>
          (datasetsAcc ++: listOf(datasetsJsonLDs(project)).generateOne,
           processRunsAcc ++: listOf(processRunsJsonLDs(project)).generateOne)
      }

      loadToStore(datasets ++: processRuns: _*)

      stats.entitiesCount.unsafeRunSync() shouldBe Map(
        KGEntityType.Dataset    -> datasets.size,
        KGEntityType.Project    -> projects.size,
        KGEntityType.ProcessRun -> processRuns.size
      )
    }

  }

  trait TestCase {
    private val logger        = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val stats =
      new StatsFinderImpl(rdfStoreConfig, logger, new SparqlQueryTimeRecorder(executionTimeRecorder))
  }

  import io.renku.jsonld.syntax._

  private def datasetsJsonLDs(datasetProjects: DatasetProject): Gen[JsonLD] =
    for {
      dataset <- datasets
    } yield toDataSetCommit(dataset.copy(projects = List(datasetProjects)))

  private def toDataSetCommit(dataSet: Dataset): JsonLD =
    dataSet.projects match {
      case project +: Nil =>
        dataSetCommit(
          committedDate = CommittedDate(project.created.date.value),
          committer     = Person(project.created.agent.name, project.created.agent.maybeEmail)
        )(
          projectPath = project.path,
          projectName = project.name
        )(
          datasetIdentifier         = dataSet.id,
          datasetTitle              = dataSet.title,
          datasetName               = dataSet.name,
          maybeDatasetUrl           = dataSet.maybeUrl,
          maybeDatasetSameAs        = None,
          maybeDatasetDescription   = dataSet.maybeDescription,
          maybeDatasetPublishedDate = dataSet.published.maybeDate,
          datasetCreatedDate        = DateCreated(project.created.date.value),
          datasetParts              = dataSet.parts.map(part => (part.name, part.atLocation))
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
}
