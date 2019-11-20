/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.datasets.rest

import java.time.LocalDate

import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.datasets.{Description, Name, PublishedDate}
import ch.datascience.graph.model.users.{Name => UserName}
import ch.datascience.http.rest.SortBy.Direction
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.CreatorsFinder
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model.{Dataset, DatasetCreator}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.{DatasetSearchResult, ProjectsCount}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort._
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.stubbing.ExternalServiceStubbing
import eu.timepit.refined.api.Refined
import io.circe.Json
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IODatasetsFinderSpec
    extends WordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks {

  "findDatasets" should {

    s"return datasets with name, description or creator matching the given phrase sorted by $NameProperty" in new TestCase {
      forAll(datasets, datasets, datasets) { (dataset1Orig, dataset2Orig, dataset3Orig) =>
        val phrase                         = phrases.generateOne
        val (dataset1, dataset2, dataset3) = storeDatasets(phrase, dataset1Orig, dataset2Orig, dataset3Orig)

        datasetsFinder
          .findDatasets(phrase, Sort.By(NameProperty, Direction.Asc))
          .unsafeRunSync() shouldBe List(
          DatasetSearchResult(dataset1.id,
                              dataset1.name,
                              dataset1.maybeDescription,
                              dataset1.published,
                              ProjectsCount(dataset1.project.size)),
          DatasetSearchResult(dataset2.id,
                              dataset2.name,
                              dataset2.maybeDescription,
                              dataset2.published,
                              ProjectsCount(dataset2.project.size)),
          DatasetSearchResult(dataset3.id,
                              dataset3.name,
                              dataset3.maybeDescription,
                              dataset3.published,
                              ProjectsCount(dataset3.project.size))
        ).sortBy(_.name.value)
      }
    }

    s"return datasets with name, description or creator matching the given phrase sorted by $DatePublishedProperty" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) = storeDatasets(
        phrase,
        datasets.generateOne changePublishedDateTo Some(PublishedDate(LocalDate.now() minusDays 1)),
        datasets.generateOne changePublishedDateTo None,
        datasets.generateOne changePublishedDateTo Some(PublishedDate(LocalDate.now()))
      )

      datasetsFinder
        .findDatasets(phrase, Sort.By(DatePublishedProperty, Direction.Desc))
        .unsafeRunSync() shouldBe List(
        DatasetSearchResult(dataset3.id,
                            dataset3.name,
                            dataset3.maybeDescription,
                            dataset3.published,
                            ProjectsCount(dataset3.project.size)),
        DatasetSearchResult(dataset1.id,
                            dataset1.name,
                            dataset1.maybeDescription,
                            dataset1.published,
                            ProjectsCount(dataset1.project.size)),
        DatasetSearchResult(dataset2.id,
                            dataset2.name,
                            dataset2.maybeDescription,
                            dataset2.published,
                            ProjectsCount(dataset2.project.size))
      )
    }

    s"return datasets with name, description or creator matching the given phrase sorted by $ProjectsCountProperty" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) = storeDatasets(
        phrase,
        datasets.generateOne changeProjectsCountTo ProjectsCount(5),
        datasets.generateOne changeProjectsCountTo ProjectsCount(0),
        datasets.generateOne changeProjectsCountTo ProjectsCount(2)
      )

      datasetsFinder
        .findDatasets(phrase, Sort.By(ProjectsCountProperty, Direction.Asc))
        .unsafeRunSync() shouldBe List(
        DatasetSearchResult(dataset2.id,
                            dataset2.name,
                            dataset2.maybeDescription,
                            dataset2.published,
                            ProjectsCount(dataset2.project.size)),
        DatasetSearchResult(dataset3.id,
                            dataset3.name,
                            dataset3.maybeDescription,
                            dataset3.published,
                            ProjectsCount(dataset3.project.size)),
        DatasetSearchResult(dataset1.id,
                            dataset1.name,
                            dataset1.maybeDescription,
                            dataset1.published,
                            ProjectsCount(dataset1.project.size))
      )
    }

    "return no results if there's no datasets with name, description or creator matching the given phrase" in new TestCase {

      loadToStore(
        triples(
          singleFileAndCommitWithDataset(projectPaths.generateOne)
        )
      )

      datasetsFinder
        .findDatasets(phrases.generateOne, searchEndpointSorts.generateOne)
        .unsafeRunSync() shouldBe empty
    }
  }

  private trait TestCase {
    private val logger = TestLogger[IO]()
    val datasetsFinder = new IODatasetsFinder(
      rdfStoreConfig,
      new CreatorsFinder(rdfStoreConfig, renkuBaseUrl, logger),
      logger
    )
  }

  private def storeDatasets(
      containtingPhrase: Phrase,
      dataset1Orig:      Dataset,
      dataset2Orig:      Dataset,
      dataset3Orig:      Dataset
  ): (Dataset, Dataset, Dataset) = {
    val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(containtingPhrase.toString)
    val dataset1 = dataset1Orig.copy(
      name = sentenceContaining(nonEmptyPhrase).map(_.value).map(Name.apply).generateOne
    )
    val dataset2 = dataset2Orig.copy(
      name             = sentenceContaining(nonEmptyPhrase).map(_.value).map(Name.apply).generateOne,
      maybeDescription = Some(sentenceContaining(nonEmptyPhrase).map(_.value).map(Description.apply).generateOne)
    )
    val dataset3 = dataset3Orig.copy(
      published = dataset3Orig.published.copy(
        creators = Set(
          DatasetCreator(
            Gen.option(emails).generateOne,
            sentenceContaining(nonEmptyPhrase).map(_.value).map(UserName.apply).generateOne
          )
        )
      )
    )

    loadToStore(
      triples(
        toSingleFileAndCommitWithDataset(dataset1),
        toSingleFileAndCommitWithDataset(dataset2),
        toSingleFileAndCommitWithDataset(dataset3),
        singleFileAndCommitWithDataset(projectPaths.generateOne)
      )
    )

    (dataset1, dataset2, dataset3)
  }

  private def toSingleFileAndCommitWithDataset(dataset: Dataset): List[Json] = dataset.project match {
    case Nil =>
      singleFileAndCommitWithDataset(
        projectPath               = projectPaths.generateOne,
        datasetIdentifier         = dataset.id,
        datasetName               = dataset.name,
        maybeDatasetDescription   = dataset.maybeDescription,
        maybeDatasetPublishedDate = dataset.published.maybeDate,
        maybeDatasetCreators      = dataset.published.creators.map(creator => (creator.name, creator.maybeEmail, None))
      ) flatMap unlinkDatasetFromProject
    case projects =>
      projects.flatMap { project =>
        singleFileAndCommitWithDataset(
          projectPath               = project.path,
          datasetIdentifier         = dataset.id,
          datasetName               = dataset.name,
          maybeDatasetDescription   = dataset.maybeDescription,
          maybeDatasetPublishedDate = dataset.published.maybeDate,
          maybeDatasetCreators      = dataset.published.creators.map(creator => (creator.name, creator.maybeEmail, None))
        )
      }
  }

  private def unlinkDatasetFromProject(json: Json) =
    if (json.hcursor.downField("http://schema.org/identifier").as[Option[String]].exists(_.isDefined))
      json.hcursor.downField("http://schema.org/isPartOf").delete.top
    else Some(json)

  private implicit class DatasetOps(dataset: Dataset) {

    def changePublishedDateTo(maybeDate: Option[PublishedDate]): Dataset =
      dataset.copy(published = dataset.published.copy(maybeDate = maybeDate))

    def changeProjectsCountTo(projectsCount: ProjectsCount): Dataset =
      dataset.copy(project = Gen.listOfN(projectsCount.value, datasetProjects).generateOne)
  }
}
