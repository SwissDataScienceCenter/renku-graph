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
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyList
import ch.datascience.graph.model.datasets.DateCreated
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators.{datasetProjects, datasets}
import ch.datascience.knowledgegraph.datasets.model.{Dataset, DatasetProject}
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.{Person, Project}
import ch.datascience.rdfstore.entities.bundles.renkuBaseUrl
import ch.datascience.rdfstore.entities.bundles.dataSetCommit
import ch.datascience.rdfstore.{FusekiBaseUrl, InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.jsonld.JsonLD
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StatsFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {
  "entitiesCount" should {

    "return info about number of objects grouped by types" in new TestCase {
      val project  = datasetProjects.generateOne
      val datasets = nonEmptyList(datasetsJsonLDs(project)).generateOne

      loadToStore(datasets.toList: _*)

      stats.entitiesCount.unsafeRunSync() shouldBe Map(
        KGEntityType.Dataset    -> datasets.size,
        KGEntityType.Project    -> 1,
        KGEntityType.ProcessRun -> 0
      )
    }

  }

  trait TestCase {
    private val fusekiBaseUrl  = FusekiBaseUrl("http://localhost")
    private val rdfStoreConfig = rdfStoreConfigs.generateOne.copy(fusekiBaseUrl = fusekiBaseUrl)
    private val logger         = TestLogger[IO]()
    val executionTimeRecorder  = TestExecutionTimeRecorder[IO](logger)
    val stats =
      new StatsFinderImpl(rdfStoreConfig, logger, new SparqlQueryTimeRecorder(executionTimeRecorder))
  }

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
}
