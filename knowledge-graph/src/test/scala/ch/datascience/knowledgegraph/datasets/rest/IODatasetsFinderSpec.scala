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

package ch.datascience.knowledgegraph.datasets.rest

import java.time.LocalDate

import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DateCreated, Description, Name, PublishedDate, SameAs}
import ch.datascience.graph.model.users.{Name => UserName}
import ch.datascience.http.rest.SortBy.Direction
import ch.datascience.http.rest.paging.PagingRequest
import ch.datascience.http.rest.paging.model.{Page, PerPage, Total}
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.CreatorsFinder
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model.{Dataset, DatasetCreator}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.{DatasetSearchResult, ProjectsCount}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort._
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.jsonld.JsonLD
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IODatasetsFinderSpec extends WordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks {

  "findDatasets" should {

    Option(Phrase("*")) +: Option.empty[Phrase] +: Nil foreach { maybePhrase =>
      s"return all datasets when the given phrase is $maybePhrase" in new TestCase {
        val datasetsList = nonEmptyList(datasets).generateOne.toList

        loadToStore(datasetsList flatMap toDataSetCommit: _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
          .unsafeRunSync()

        result.results shouldBe datasetsList
          .map(_.toDatasetSearchResult)
          .sortBy(_.name.value)

        result.pagingInfo.total shouldBe Total(datasetsList.size)
      }
    }

    "merge all datasets having the same sameAs not pointing to project's dataset - case when no phrase is given" in new TestCase {
      val dataset1 = datasets(
        maybeSameAs = datasetSameAs.toGeneratorOfSomes,
        projects    = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne
      val dataset2     = datasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne
      val datasetsList = List(dataset1, dataset2)

      loadToStore(datasetsList flatMap toDataSetCommit: _*)

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()

      result.results shouldBe datasetsList
        .map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(datasetsList.size)
    }

    "merge datasets when they are imported from other renku project - case when no phrase is given" in new TestCase {
      val dataset1 = datasets(
        maybeSameAs = emptyOptionOf[SameAs],
        projects    = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne
      val dataset2     = datasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne
      val datasetsList = List(dataset1, dataset2)

      loadToStore(datasetsList flatMap toDataSetCommit: _*)

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()

      result.results shouldBe datasetsList
        .map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(datasetsList.size)
    }

    "return datasets having neither sameAs nor imported to other projects - case when no phrase is given" in new TestCase {
      val dataset1 = datasets(maybeSameAs = emptyOptionOf[SameAs],
                              projects = nonEmptyList(datasetProjects, minElements = 2)).generateOne
      val dataset2 = datasets(maybeSameAs = emptyOptionOf[SameAs],
                              projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne
      val dataset3 = datasets(maybeSameAs = datasetSameAs.toGeneratorOfSomes,
                              projects = nonEmptyList(datasetProjects, minElements = 2)).generateOne
      val datasetsList = List(dataset1, dataset2, dataset3)

      loadToStore(datasetsList flatMap toDataSetCommit: _*)

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()

      result.results shouldBe datasetsList
        .map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(datasetsList.size)
    }

    "merge all datasets having the same sameAs not pointing to project's dataset - case when some phrase is given" in new TestCase {

      val phrase = phrases.generateOne
      val dataset1 = datasets(
        maybeSameAs = datasetSameAs.toGeneratorOfSomes,
        projects    = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset2 = datasets(
        maybeSameAs = datasetSameAs.toGeneratorOfSomes,
        projects    = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeDescContaining(phrase)
      val dataset3 = datasets(
        maybeSameAs = datasetSameAs.toGeneratorOfSomes,
        projects    = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeCreatorNameContaining(phrase)

      loadToStore(List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap toDataSetCommit: _*)

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(NameProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      val matchingDatasets = List(dataset1, dataset2, dataset3)
      result.results shouldBe List(matchingDatasets.sorted(byName)(1).toDatasetSearchResult)

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    "merge datasets when they are imported from other renku project - case when some phrase is given" in new TestCase {

      val phrase = phrases.generateOne
      val dataset1 = datasets(
        maybeSameAs = emptyOptionOf[SameAs],
        projects    = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset2 = datasets(
        maybeSameAs = emptyOptionOf[SameAs],
        projects    = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeDescContaining(phrase)
      val dataset3 = datasets(
        maybeSameAs = emptyOptionOf[SameAs],
        projects    = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeCreatorNameContaining(phrase)

      loadToStore(List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap toDataSetCommit: _*)

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(NameProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      val matchingDatasets = List(dataset1, dataset2, dataset3)
      result.results shouldBe List(matchingDatasets.sorted(byName)(1).toDatasetSearchResult)

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    "return datasets having neither sameAs nor imported to other projects - case when some phrase is given" in new TestCase {

      val phrase = phrases.generateOne
      val dataset1 = datasets(
        maybeSameAs = emptyOptionOf[SameAs],
        projects    = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset2 = datasets(
        maybeSameAs = emptyOptionOf[SameAs],
        projects    = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeDescContaining(phrase)
      val dataset3 = datasets(
        maybeSameAs = datasetSameAs.toGeneratorOfSomes,
        projects    = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeCreatorNameContaining(phrase)

      loadToStore(List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap toDataSetCommit: _*)

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(NameProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      val matchingDatasets = List(dataset1, dataset2, dataset3)
      result.results shouldBe List(matchingDatasets.sorted(byName)(1).toDatasetSearchResult)

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    s"return datasets with name, description or creator matching the given phrase sorted by $NameProperty" in new TestCase {
      forAll(datasets, datasets, datasets, datasets) { (dataset1Orig, dataset2Orig, dataset3Orig, nonPhrased) =>
        val phrase                         = phrases.generateOne
        val (dataset1, dataset2, dataset3) = addPhrase(phrase, dataset1Orig, dataset2Orig, dataset3Orig)

        loadToStore(List(dataset1, dataset2, dataset3, nonPhrased) flatMap toDataSetCommit: _*)

        datasetsFinder
          .findDatasets(Some(phrase), Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
          .unsafeRunSync()
          .results shouldBe List(dataset1.toDatasetSearchResult,
                                 dataset2.toDatasetSearchResult,
                                 dataset3.toDatasetSearchResult).sortBy(_.name.value)
      }
    }

    s"return datasets with name, description or creator matching the given phrase sorted by $DatePublishedProperty" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) = addPhrase(
        phrase,
        datasets.generateOne changePublishedDateTo Some(PublishedDate(LocalDate.now() minusDays 1)),
        datasets.generateOne changePublishedDateTo None,
        datasets.generateOne changePublishedDateTo Some(PublishedDate(LocalDate.now()))
      )

      loadToStore(List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap toDataSetCommit: _*)

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(DatePublishedProperty, Direction.Desc), PagingRequest.default)
        .unsafeRunSync()
        .results shouldBe List(dataset3.toDatasetSearchResult,
                               dataset1.toDatasetSearchResult,
                               dataset2.toDatasetSearchResult)
    }

    s"return datasets with name, description or creator matching the given phrase sorted by $ProjectsCountProperty" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) = addPhrase(
        phrase,
        datasets(projects = nonEmptyList(datasetProjects, minElements = 4, maxElements = 4)).generateOne,
        datasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne,
        datasets(projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)).generateOne
      )

      loadToStore(List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap toDataSetCommit: _*)

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(ProjectsCountProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()
        .results shouldBe List(dataset2.toDatasetSearchResult,
                               dataset3.toDatasetSearchResult,
                               dataset1.toDatasetSearchResult)
    }

    "return the requested page of datasets matching the given phrase" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) =
        addPhrase(phrase, datasets.generateOne, datasets.generateOne, datasets.generateOne)

      loadToStore(List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap toDataSetCommit: _*)

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(NameProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      val expectedDataset = List(dataset1, dataset2, dataset3).sorted(byName)(1)
      result.results shouldBe List(expectedDataset.toDatasetSearchResult)

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    "return no results if the requested page does not exist" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) =
        addPhrase(phrase, datasets.generateOne, datasets.generateOne, datasets.generateOne)

      loadToStore(List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap toDataSetCommit: _*)

      val pagingRequest = PagingRequest(Page(2), PerPage(3))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(NameProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      result.results                  shouldBe Nil
      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    "return no results if there's no datasets with name, description or creator matching the given phrase" in new TestCase {

      loadToStore(randomDataSetCommit)

      val result = datasetsFinder
        .findDatasets(Some(phrases.generateOne), searchEndpointSorts.generateOne, PagingRequest.default)
        .unsafeRunSync()

      result.results          shouldBe Nil
      result.pagingInfo.total shouldBe Total(0)
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

  private def addPhrase(
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
      maybeDescription = Some(sentenceContaining(nonEmptyPhrase).map(_.value).map(Description.apply).generateOne)
    )
    val dataset3 = dataset3Orig.copy(
      published = dataset3Orig.published.copy(
        creators = Set(
          DatasetCreator(
            emails.generateOption,
            sentenceContaining(nonEmptyPhrase).map(_.value).map(UserName.apply).generateOne,
            affiliations.generateOption
          )
        )
      )
    )

    (dataset1, dataset2, dataset3)
  }

  private def toDataSetCommit(dataSet: Dataset): List[JsonLD] = dataSet.project match {
    case firstProject +: otherProjects =>
      val createdDate = datasetCreatedDates.generateOne
      val firstJsonLd = dataSetCommit()(
        projectPath = firstProject.path
      )(
        datasetIdentifier         = dataSet.id,
        datasetName               = dataSet.name,
        maybeDatasetSameAs        = dataSet.maybeSameAs,
        maybeDatasetDescription   = dataSet.maybeDescription,
        maybeDatasetPublishedDate = dataSet.published.maybeDate,
        datasetCreatedDate        = createdDate,
        datasetCreators           = dataSet.published.creators map toPerson
      )

      val maybeSameAs = dataSet.maybeSameAs orElse firstJsonLd.entityId.flatMap(id => SameAs.fromId(id.value).toOption)
      val otherJsonLds = otherProjects.map { project =>
        dataSetCommit()(
          projectPath = project.path
        )(
          datasetName               = dataSet.name,
          maybeDatasetSameAs        = maybeSameAs,
          maybeDatasetDescription   = dataSet.maybeDescription,
          maybeDatasetPublishedDate = dataSet.published.maybeDate,
          datasetCreatedDate        = DateCreated(createdDate.value.plusSeconds(positiveInts().generateOne.value)),
          datasetCreators           = dataSet.published.creators map toPerson
        )
      }

      firstJsonLd +: otherJsonLds
  }

  private implicit class DatasetOps(dataset: Dataset) {

    def changePublishedDateTo(maybeDate: Option[PublishedDate]): Dataset =
      dataset.copy(published = dataset.published.copy(maybeDate = maybeDate))

    def makeNameContaining(phrase: Phrase): Dataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataset.copy(
        name = sentenceContaining(nonEmptyPhrase).map(_.value).map(Name.apply).generateOne
      )
    }

    def makeCreatorNameContaining(phrase: Phrase): Dataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataset.copy(
        maybeDescription = sentenceContaining(nonEmptyPhrase).map(_.value).map(Description.apply).generateSome
      )
    }

    def makeDescContaining(phrase: Phrase): Dataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataset.copy(
        published = dataset.published.copy(
          creators = Set(
            DatasetCreator(
              emails.generateOption,
              sentenceContaining(nonEmptyPhrase).map(_.value).map(UserName.apply).generateOne,
              affiliations.generateOption
            )
          )
        )
      )
    }

    lazy val toDatasetSearchResult: DatasetSearchResult = DatasetSearchResult(
      dataset.id,
      dataset.name,
      dataset.maybeDescription,
      dataset.published,
      ProjectsCount(dataset.project.size)
    )
  }

  private lazy val toPerson: DatasetCreator => Person =
    creator => Person(creator.name, creator.maybeEmail, creator.maybeAffiliation)

  private lazy val byName: Ordering[Dataset] =
    (ds1: Dataset, ds2: Dataset) => ds1.name.value compareTo ds2.name.value
}
