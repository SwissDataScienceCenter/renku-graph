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
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DateCreated, Description, PublishedDate, SameAs, Title}
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.graph.model.users.{Name => UserName}
import ch.datascience.http.rest.SortBy.Direction
import ch.datascience.http.rest.paging.PagingRequest
import ch.datascience.http.rest.paging.model.{Page, PerPage, Total}
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model.{Dataset, DatasetCreator, DatasetProject}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.{DatasetSearchResult, ProjectsCount}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.entities.{DataSet, Person}
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.jsonld.{EntityId, JsonLD}
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IODatasetsFinderSpec
    extends AnyWordSpec
    with InMemoryRdfStore
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "findDatasets - no phrase" should {

    Option(Phrase("*")) +: Option.empty[Phrase] +: Nil foreach { maybePhrase =>
      s"return all datasets when the given phrase is $maybePhrase" in new TestCase {

        val datasetsList = nonEmptyList(datasets, maxElements = 1).generateOne.toList

        loadToStore(datasetsList flatMap (_.toJsonLD(noSameAs = false)): _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase = maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
          .unsafeRunSync()

        result.results shouldBe datasetsList
          .map(_.toDatasetSearchResult)
          .sortBy(_.title.value)

        result.pagingInfo.total shouldBe Total(datasetsList.size)
      }
    }

    "merge all datasets having the same sameAs not pointing to project's dataset" in new TestCase {
      val dataset1 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne
      val dataset2     = datasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne
      val datasetsList = List(dataset1, dataset2)

      loadToStore(datasetsList flatMap (_.toJsonLD(noSameAs = false)): _*)

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(TitleProperty, Direction.Asc), PagingRequest(Page(1), PerPage(2)))
        .unsafeRunSync()

      result.results shouldBe datasetsList
        .map(_.toDatasetSearchResult)
        .sortBy(_.title.value)

      result.pagingInfo.total shouldBe Total(datasetsList.size)
    }

    "merge datasets when they are imported from other renku project" in new TestCase {
      val dataset1 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne
      val dataset2     = datasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne
      val datasetsList = List(dataset1, dataset2)

      loadToStore(datasetsList flatMap (_.toJsonLD(noSameAs = true)): _*)

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(TitleProperty, Direction.Asc), PagingRequest(Page(1), PerPage(2)))
        .unsafeRunSync()

      result.results shouldBe datasetsList
        .map(_.toDatasetSearchResult)
        .sortBy(_.title.value)

      result.pagingInfo.total shouldBe Total(datasetsList.size)
    }

    "return datasets having neither sameAs nor imported to other projects" in new TestCase {
      val dataset1 = datasets(projects = nonEmptyList(datasetProjects, minElements = 2)).generateOne
      val dataset2 = datasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne
      val dataset3 = datasets(projects = nonEmptyList(datasetProjects, minElements = 2)).generateOne

      loadToStore(
        List(
          dataset1.toJsonLD(noSameAs = true),
          dataset2.toJsonLD(noSameAs = true),
          dataset3.toJsonLD(noSameAs = false)
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(TitleProperty, Direction.Asc), PagingRequest(Page(1), PerPage(3)))
        .unsafeRunSync()

      val datasetsList = List(dataset1, dataset2, dataset3)
      result.results shouldBe datasetsList
        .map(_.toDatasetSearchResult)
        .sortBy(_.title.value)

      result.pagingInfo.total shouldBe Total(datasetsList.size)
    }
  }

  "findDatasets in case of forks - no phrase" should {

    "merge all datasets having the same sameAs together with forks to different projects" in new TestCase {
      val dataset1CreatedDate = datasetCreatedDates.generateOne
      val dataset1 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
      ).generateOne
      val dataset1Fork = dataset1.copy(projects = List(datasetProjects.generateOne))

      val dataset2CreatedDate = dataset1CreatedDate.shiftToFuture
      val dataset2 = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        projects = List(datasetProjects.generateOne)
      )
      val dataset2Fork = dataset2.copy(projects = List(datasetProjects.generateOne))

      loadToStore(
        List(
          dataset1.toJsonLD(dataset1CreatedDate, noSameAs     = false),
          dataset1Fork.toJsonLD(dataset1CreatedDate, noSameAs = false),
          dataset2.toJsonLD(dataset2CreatedDate, noSameAs     = false),
          dataset2Fork.toJsonLD(dataset2CreatedDate, noSameAs = false)
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(TitleProperty, Direction.Asc), PagingRequest(Page(1), PerPage(1)))
        .unsafeRunSync()

      result.results shouldBe List(
        dataset1 addAll dataset1Fork.projects addAll dataset2.projects addAll dataset2Fork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.title.value)

      result.pagingInfo.total shouldBe Total(1)
    }

    "merge datasets imported from other renku project and forked to different projects" in new TestCase {
      val initialDatasetCreatedDate = datasetCreatedDates.generateOne
      val initialDataset = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 3, maxElements = 3)
      ).generateOne
      val initialDatasetFork = initialDataset.copy(projects = List(datasetProjects.generateOne))

      val importedDatasetCreatedDate = initialDatasetCreatedDate.shiftToFuture
      val importedDataset = initialDataset.copy(
        id       = datasetIdentifiers.generateOne,
        projects = List(datasetProjects.generateOne),
        sameAs   = initialDataset.entityId.asSameAs
      )
      val importedDatasetFork = importedDataset.copy(projects = List(datasetProjects.generateOne))

      loadToStore(
        List(
          initialDataset.toJsonLD(initialDatasetCreatedDate, noSameAs       = true),
          initialDatasetFork.toJsonLD(initialDatasetCreatedDate, noSameAs   = true),
          importedDataset.toJsonLD(importedDatasetCreatedDate, noSameAs     = false),
          importedDatasetFork.toJsonLD(importedDatasetCreatedDate, noSameAs = false)
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(TitleProperty, Direction.Asc), PagingRequest(Page(1), PerPage(1)))
        .unsafeRunSync()

      result.results shouldBe List(
        initialDataset addAll initialDatasetFork.projects addAll importedDataset.projects addAll importedDatasetFork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.title.value)

      result.pagingInfo.total shouldBe Total(1)
    }

    "merge non-imported and not being imported datasets when they are forked to different projects" in new TestCase {
      val dataset1CreatedDate = datasetCreatedDates.generateOne
      val dataset1 = datasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne
      val dataset1Fork = dataset1.copy(projects = List(datasetProjects.generateOne))

      val dataset2CreatedDate = datasetCreatedDates.generateOne
      val dataset2 = datasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne
      val dataset2Fork = dataset2.copy(projects = List(datasetProjects.generateOne))

      loadToStore(
        List(
          dataset1.toJsonLD(dataset1CreatedDate, noSameAs     = true),
          dataset1Fork.toJsonLD(dataset1CreatedDate, noSameAs = true),
          dataset2.toJsonLD(dataset2CreatedDate, noSameAs     = true),
          dataset2Fork.toJsonLD(dataset2CreatedDate, noSameAs = true)
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(TitleProperty, Direction.Asc), PagingRequest(Page(1), PerPage(2)))
        .unsafeRunSync()

      result.results shouldBe List(
        dataset1 addAll dataset1Fork.projects,
        dataset2 addAll dataset2Fork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.title.value)

      result.pagingInfo.total shouldBe Total(2)
    }
  }

  "findDatasets - some phrase given" should {

    "merge all datasets having the same sameAs not pointing to project's dataset" in new TestCase {

      val phrase = phrases.generateOne
      val dataset1 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset2 = datasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeDescContaining(phrase)
      val dataset3 = datasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeCreatorNameContaining(phrase)

      loadToStore(
        List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap (_.toJsonLD(noSameAs = false)): _*
      )

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      val matchingDatasets = List(dataset1, dataset2, dataset3)
      result.results shouldBe List(matchingDatasets.sorted(byName)(1).toDatasetSearchResult)

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    "merge datasets when they are imported from other renku project" in new TestCase {

      val phrase = phrases.generateOne
      val dataset1 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset2 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeDescContaining(phrase)
      val dataset3 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeCreatorNameContaining(phrase)

      loadToStore(
        List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap (_.toJsonLD(noSameAs = true)): _*
      )

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      val matchingDatasets = List(dataset1, dataset2, dataset3)
      result.results shouldBe List((matchingDatasets sorted byName)(1).toDatasetSearchResult)

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    "return datasets having neither sameAs nor imported to other projects" in new TestCase {

      val phrase = phrases.generateOne
      val dataset1 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset2 = datasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeDescContaining(phrase)
      val dataset3 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeCreatorNameContaining(phrase)

      loadToStore(
        List(
          dataset1.toJsonLD(noSameAs             = true),
          dataset2.toJsonLD(noSameAs             = true),
          dataset3.toJsonLD(noSameAs             = false),
          datasets.generateOne.toJsonLD(noSameAs = false)
        ).flatten: _*
      )

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      val matchingDatasets = List(dataset1, dataset2, dataset3)
      result.results shouldBe List(matchingDatasets.sorted(byName)(1).toDatasetSearchResult)

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }
  }

  "findDatasets in case of forks - some phrase given" should {

    "merge all datasets having the same sameAs together with forks to different projects" in new TestCase {
      val phrase = phrases.generateOne

      val dataset1CreatedDate = datasetCreatedDates.generateOne
      val dataset1 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset1Fork            = dataset1.copy(projects = List(datasetProjects.generateOne))
      val dataset1PrimCreatedDate = dataset1CreatedDate.shiftToFuture
      val dataset1Prim = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        projects = List(datasetProjects.generateOne)
      )
      val dataset1PrimFork    = dataset1Prim.copy(projects = List(datasetProjects.generateOne))
      val dataset2CreatedDate = datasetCreatedDates.generateOne
      val dataset2 = datasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeDescContaining(phrase)
      val dataset2Fork        = dataset2.copy(projects = List(datasetProjects.generateOne))
      val dataset3CreatedDate = datasetCreatedDates.generateOne
      val dataset3 = datasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeCreatorNameContaining(phrase)
      val dataset3Fork = dataset3.copy(projects = List(datasetProjects.generateOne))

      loadToStore(
        List(
          dataset1.toJsonLD(dataset1CreatedDate, noSameAs             = false),
          dataset1Fork.toJsonLD(dataset1CreatedDate, noSameAs         = false),
          dataset1Prim.toJsonLD(dataset1PrimCreatedDate, noSameAs     = false),
          dataset1PrimFork.toJsonLD(dataset1PrimCreatedDate, noSameAs = false),
          dataset2.toJsonLD(dataset2CreatedDate, noSameAs             = false),
          dataset2Fork.toJsonLD(dataset2CreatedDate, noSameAs         = false),
          dataset3.toJsonLD(dataset3CreatedDate, noSameAs             = false),
          dataset3Fork.toJsonLD(dataset3CreatedDate, noSameAs         = false)
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = Some(phrase),
                      Sort.By(TitleProperty, Direction.Asc),
                      PagingRequest(Page(1), PerPage(3)))
        .unsafeRunSync()

      result.results shouldBe List(
        dataset1 addAll dataset1Fork.projects addAll dataset1Prim.projects addAll dataset1PrimFork.projects,
        dataset2 addAll dataset2Fork.projects,
        dataset3 addAll dataset3Fork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.title.value)

      result.pagingInfo.total shouldBe Total(3)
    }

    "merge datasets imported from other renku project and forked to different projects" in new TestCase {
      val phrase = phrases.generateOne

      val dataset1CreatedDate = datasetCreatedDates.generateOne
      val dataset1 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset1Fork            = dataset1.copy(projects = List(datasetProjects.generateOne))
      val dataset1PrimCreatedDate = dataset1CreatedDate.shiftToFuture
      val dataset1Prim = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        projects = List(datasetProjects.generateOne),
        sameAs   = dataset1.entityId.asSameAs
      )
      val dataset1PrimFork    = dataset1Prim.copy(projects = List(datasetProjects.generateOne))
      val dataset2CreatedDate = datasetCreatedDates.generateOne
      val dataset2 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeDescContaining(phrase)
      val dataset2Fork        = dataset2.copy(projects = List(datasetProjects.generateOne))
      val dataset3CreatedDate = datasetCreatedDates.generateOne
      val dataset3 = datasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeCreatorNameContaining(phrase)
      val dataset3Fork = dataset3.copy(projects = List(datasetProjects.generateOne))

      loadToStore(
        List(
          dataset1.toJsonLD(dataset1CreatedDate, noSameAs             = true),
          dataset1Fork.toJsonLD(dataset1CreatedDate, noSameAs         = true),
          dataset1Prim.toJsonLD(dataset1PrimCreatedDate, noSameAs     = false),
          dataset1PrimFork.toJsonLD(dataset1PrimCreatedDate, noSameAs = false),
          dataset2.toJsonLD(dataset2CreatedDate, noSameAs             = true),
          dataset2Fork.toJsonLD(dataset2CreatedDate, noSameAs         = true),
          dataset3.toJsonLD(dataset3CreatedDate, noSameAs             = true),
          dataset3Fork.toJsonLD(dataset3CreatedDate, noSameAs         = true)
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = Some(phrase),
                      Sort.By(TitleProperty, Direction.Asc),
                      PagingRequest(Page(1), PerPage(3)))
        .unsafeRunSync()

      result.results shouldBe List(
        dataset1 addAll dataset1Fork.projects addAll dataset1Prim.projects addAll dataset1PrimFork.projects,
        dataset2 addAll dataset2Fork.projects,
        dataset3 addAll dataset3Fork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.title.value)

      result.pagingInfo.total shouldBe Total(3)
    }

    "merge non-imported and not being imported datasets when they are forked to different projects" in new TestCase {
      val phrase = phrases.generateOne

      val dataset1CreatedDate = datasetCreatedDates.generateOne
      val dataset1 = datasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeNameContaining(phrase)
      val dataset1Fork = dataset1.copy(projects = List(datasetProjects.generateOne))

      val dataset2CreatedDate = datasetCreatedDates.generateOne
      val dataset2 = datasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeDescContaining(phrase)
      val dataset2Fork = dataset2.copy(projects = List(datasetProjects.generateOne))

      val dataset3CreatedDate = datasetCreatedDates.generateOne
      val dataset3 = datasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeCreatorNameContaining(phrase)
      val dataset3Fork = dataset3.copy(projects = List(datasetProjects.generateOne))

      loadToStore(
        List(
          dataset1.toJsonLD(dataset1CreatedDate, noSameAs     = true),
          dataset1Fork.toJsonLD(dataset1CreatedDate, noSameAs = true),
          dataset2.toJsonLD(dataset2CreatedDate, noSameAs     = true),
          dataset2Fork.toJsonLD(dataset2CreatedDate, noSameAs = true),
          dataset3.toJsonLD(dataset3CreatedDate, noSameAs     = true),
          dataset3Fork.toJsonLD(dataset3CreatedDate, noSameAs = true)
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = Some(phrase),
                      Sort.By(TitleProperty, Direction.Asc),
                      PagingRequest(Page(1), PerPage(3)))
        .unsafeRunSync()

      result.results shouldBe List(
        dataset1 addAll dataset1Fork.projects,
        dataset2 addAll dataset2Fork.projects,
        dataset3 addAll dataset3Fork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.title.value)

      result.pagingInfo.total shouldBe Total(3)
    }
  }

  "findDatasets with explicit sorting given" should {

    s"return datasets with name, description or creator matching the given phrase sorted by $TitleProperty" in new TestCase {
      forAll(datasets, datasets, datasets, datasets) { (dataset1Orig, dataset2Orig, dataset3Orig, nonPhrased) =>
        val phrase                         = phrases.generateOne
        val (dataset1, dataset2, dataset3) = addPhrase(phrase, dataset1Orig, dataset2Orig, dataset3Orig)

        loadToStore(List(dataset1, dataset2, dataset3, nonPhrased) flatMap (_.toJsonLD(noSameAs = false)): _*)

        datasetsFinder
          .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
          .unsafeRunSync()
          .results shouldBe List(dataset1.toDatasetSearchResult,
                                 dataset2.toDatasetSearchResult,
                                 dataset3.toDatasetSearchResult).sortBy(_.title.value)
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

      loadToStore(
        List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap (_.toJsonLD(noSameAs = false)): _*
      )

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

      loadToStore(
        List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap (_.toJsonLD(noSameAs = false)): _*
      )

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(ProjectsCountProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()
        .results shouldBe List(dataset2.toDatasetSearchResult,
                               dataset3.toDatasetSearchResult,
                               dataset1.toDatasetSearchResult)
    }
  }

  "findDatasets with explicit paging request" should {

    "return the requested page of datasets matching the given phrase" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) =
        addPhrase(phrase, datasets.generateOne, datasets.generateOne, datasets.generateOne)

      loadToStore(
        List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap (_.toJsonLD(noSameAs = false)): _*
      )

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), pagingRequest)
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

      loadToStore(
        List(dataset1, dataset2, dataset3, datasets.generateOne) flatMap (_.toJsonLD(noSameAs = false)): _*
      )

      val pagingRequest = PagingRequest(Page(2), PerPage(3))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), pagingRequest)
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

  "findDatasets in the case there's datasets import hierarchy" should {

    emptyOptionOf[Phrase] +: phrases.toGeneratorOfSomes +: Nil foreach { phraseGenerator =>
      val maybePhrase = phraseGenerator.generateOne

      s"return a single dataset - case when the first dataset is externally imported and ${maybePhrase.getOrElse("no")} phrase given" in new TestCase {
        val dataset1 = datasets(
          projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
        ).generateOne.makeDescContaining(maybePhrase)
        val dataset1CreatedDate = datasetCreatedDates.generateOne
        val dataset1Jsons       = dataset1.toJsonLD(dataset1CreatedDate, noSameAs = false)

        val dataset2 = dataset1.copy(
          id       = datasetIdentifiers.generateOne,
          sameAs   = dataset1.entityId.asSameAs,
          projects = List(datasetProjects.generateOne)
        )
        val dataset2CreatedDate = dataset1CreatedDate.shiftToFuture
        val dataset2Jsons       = dataset2.toJsonLD(dataset2CreatedDate, noSameAs = false)

        val dataset3 = dataset2.copy(
          id       = datasetIdentifiers.generateOne,
          sameAs   = dataset2.entityId.asSameAs,
          projects = List(datasetProjects.generateOne)
        )
        val dataset3Jsons = dataset3.toJsonLD(dataset2CreatedDate.shiftToFuture, noSameAs = false)

        loadToStore(dataset1Jsons ++ dataset2Jsons ++ dataset3Jsons: _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest(Page(1), PerPage(1)))
          .unsafeRunSync()

        result.results shouldBe List(
          dataset1 addAll dataset2.projects addAll dataset3.projects
        ).map(_.toDatasetSearchResult)
          .sortBy(_.title.value)

        result.pagingInfo.total shouldBe Total(1)
      }

      s"return a single dataset - case when the first dataset is in-project created and ${maybePhrase.getOrElse("no")} phrase given" in new TestCase {
        val dataset1 = datasets(
          projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
        ).generateOne.makeDescContaining(maybePhrase)
        val dataset1CreatedDate = datasetCreatedDates.generateOne
        val dataset1Jsons       = dataset1.toJsonLD(dataset1CreatedDate, noSameAs = true)

        val dataset2 = dataset1.copy(
          id       = datasetIdentifiers.generateOne,
          sameAs   = dataset1.entityId.asSameAs,
          projects = List(datasetProjects.generateOne)
        )
        val dataset2CreatedDate = dataset1CreatedDate.shiftToFuture
        val dataset2Jsons       = dataset2.toJsonLD(dataset2CreatedDate, noSameAs = false)

        val dataset3 = dataset2.copy(
          id       = datasetIdentifiers.generateOne,
          sameAs   = dataset2.entityId.asSameAs,
          projects = List(datasetProjects.generateOne)
        )
        val dataset3Jsons = dataset3.toJsonLD(dataset2CreatedDate.shiftToFuture, noSameAs = false)

        loadToStore(dataset1Jsons ++ dataset2Jsons ++ dataset3Jsons: _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest(Page(1), PerPage(1)))
          .unsafeRunSync()

        result.results shouldBe List(
          dataset1 addAll dataset2.projects addAll dataset3.projects
        ).map(_.toDatasetSearchResult)
          .sortBy(_.title.value)

        result.pagingInfo.total shouldBe Total(1)
      }

      "return a single dataset - case when there're two first level projects sharing a dataset " +
        s"and some other project imports from on of these two; ${maybePhrase.getOrElse("no")} phrase given" in new TestCase {
        val dataset1 = datasets(
          projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
        ).generateOne.makeDescContaining(maybePhrase)
        val dataset1CreatedDate = datasetCreatedDates.generateOne
        val dataset1Jsons       = dataset1.toJsonLD(dataset1CreatedDate, noSameAs = false)

        val dataset2 = datasets(
          sameAs   = Gen.const(dataset1.sameAs),
          projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
        ).generateOne
        val dataset2CreatedDate = dataset1CreatedDate.shiftToFuture
        val dataset2Jsons       = dataset2.toJsonLD(dataset2CreatedDate, noSameAs = false)

        val dataset3 = dataset2.copy(
          id       = datasetIdentifiers.generateOne,
          sameAs   = dataset2.entityId.asSameAs,
          projects = List(datasetProjects.generateOne)
        )
        val dataset3Jsons = dataset3.toJsonLD(dataset2CreatedDate.shiftToFuture, noSameAs = false)

        loadToStore(dataset1Jsons ++ dataset2Jsons ++ dataset3Jsons: _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest(Page(1), PerPage(1)))
          .unsafeRunSync()

        result.results shouldBe List(
          dataset1 addAll dataset2.projects addAll dataset3.projects
        ).map(_.toDatasetSearchResult)
          .sortBy(_.title.value)

        result.pagingInfo.total shouldBe Total(1)
      }

      "return a single dataset " +
        s"- case when there are 4 levels of inheritance and ${maybePhrase.getOrElse("no")} phrase given" in new TestCase {
        val dataset1 = datasets(
          projects = nonEmptyList(datasetProjects, maxElements = 3)
        ).generateOne.makeDescContaining(maybePhrase)
        val dataset1CreatedDate = datasetCreatedDates.generateOne
        val dataset1Jsons       = dataset1.toJsonLD(dataset1CreatedDate, noSameAs = false)

        val (allDatasets, allJsons) = nestDataset(ifLessThan = 4)(dataset1, dataset1CreatedDate, dataset1Jsons)

        loadToStore(allJsons: _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest(Page(1), PerPage(1)))
          .unsafeRunSync()

        result.results shouldBe List(
          dataset1 addAll allDatasets.tail.flatMap(_.projects)
        ).map(_.toDatasetSearchResult)
          .sortBy(_.title.value)

        result.pagingInfo.total shouldBe Total(1)
      }
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val datasetsFinder = new IODatasetsFinder(
      rdfStoreConfig,
      new CreatorsFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder),
      logger,
      timeRecorder
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
      title = sentenceContaining(nonEmptyPhrase).map(_.value).map(Title.apply).generateOne
    )
    val dataset2 = dataset2Orig.copy(
      maybeDescription = Some(sentenceContaining(nonEmptyPhrase).map(_.value).map(Description.apply).generateOne)
    )
    val dataset3 = dataset3Orig.copy(
      published = dataset3Orig.published.copy(
        creators = Set(
          DatasetCreator(
            userEmails.generateOption,
            sentenceContaining(nonEmptyPhrase).map(_.value).map(UserName.apply).generateOne,
            userAffiliations.generateOption
          )
        )
      )
    )

    (dataset1, dataset2, dataset3)
  }

  private implicit class DatasetOps(dataset: Dataset) {

    lazy val entityId: EntityId = DataSet.entityId(dataset.id)

    def changePublishedDateTo(maybeDate: Option[PublishedDate]): Dataset =
      dataset.copy(published = dataset.published.copy(maybeDate = maybeDate))

    def addAll(projects: List[DatasetProject]): Dataset =
      dataset.copy(projects = dataset.projects ++ projects)

    def makeNameContaining(phrase: Phrase): Dataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataset.copy(
        title = sentenceContaining(nonEmptyPhrase).map(_.value).map(Title.apply).generateOne
      )
    }

    def makeCreatorNameContaining(phrase: Phrase): Dataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataset.copy(
        published = dataset.published.copy(
          creators = Set(
            DatasetCreator(
              userEmails.generateOption,
              sentenceContaining(nonEmptyPhrase).map(_.value).map(UserName.apply).generateOne,
              userAffiliations.generateOption
            )
          )
        )
      )
    }

    def makeDescContaining(maybePhrase: Option[Phrase]): Dataset =
      maybePhrase map makeDescContaining getOrElse dataset

    def makeDescContaining(phrase: Phrase): Dataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataset.copy(
        maybeDescription = sentenceContaining(nonEmptyPhrase).map(_.value).map(Description.apply).generateSome
      )
    }

    lazy val toDatasetSearchResult: DatasetSearchResult = DatasetSearchResult(
      dataset.id,
      dataset.title,
      dataset.name,
      dataset.maybeDescription,
      dataset.published,
      ProjectsCount(dataset.projects.size)
    )

    def toJsonLD(noSameAs: Boolean): List[JsonLD] = toJsonLD(datasetCreatedDates.generateOne, noSameAs)

    def toJsonLD(firstDatasetDateCreated: DateCreated, noSameAs: Boolean): List[JsonLD] =
      dataset.projects match {
        case firstProject +: otherProjects =>
          val firstJsonLd = dataSetCommit(
            committedDate = CommittedDate(firstDatasetDateCreated.value)
          )(
            projectPath = firstProject.path
          )(
            datasetIdentifier         = dataset.id,
            datasetTitle              = dataset.title,
            datasetName               = dataset.name,
            maybeDatasetSameAs        = if (noSameAs) None else dataset.sameAs.some,
            maybeDatasetDescription   = dataset.maybeDescription,
            maybeDatasetPublishedDate = dataset.published.maybeDate,
            datasetCreatedDate        = firstDatasetDateCreated,
            datasetCreators           = dataset.published.creators map toPerson
          )

          val someSameAs =
            if (noSameAs) DataSet.entityId(dataset.id).asSameAs.some
            else dataset.sameAs.some
          val otherJsonLds = otherProjects.map { project =>
            val projectDateCreated = firstDatasetDateCreated.shiftToFuture
            dataSetCommit(
              committedDate = CommittedDate(projectDateCreated.value)
            )(
              projectPath = project.path
            )(
              datasetTitle              = dataset.title,
              datasetName               = dataset.name,
              maybeDatasetSameAs        = someSameAs,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreatedDate        = projectDateCreated,
              datasetCreators           = dataset.published.creators map toPerson
            )
          }

          firstJsonLd +: otherJsonLds
      }
  }

  private implicit class JsonsOps(jsons: List[JsonLD]) {
    lazy val entityId: Option[EntityId] = jsons.headOption.flatMap(_.entityId)
  }

  private implicit class EntityIdOps(entityId: EntityId) {
    lazy val asSameAs: SameAs = SameAs.fromId(entityId.value.toString).fold(throw _, identity)
  }

  private implicit class OptionEntityIdOps(maybeEntityId: Option[EntityId]) {
    lazy val asSameAs: SameAs = maybeEntityId
      .flatMap(id => SameAs.fromId(id.value.toString).toOption)
      .getOrElse(throw new Exception(s"Cannot convert $maybeEntityId EntityId to SameAs"))
  }

  @scala.annotation.tailrec
  private def nestDataset(ifLessThan: Int)(
      dataset:                        Dataset,
      createdDate:                    DateCreated,
      datasetJsons:                   List[JsonLD],
      accumulator:                    (List[Dataset], List[JsonLD]) = Nil -> Nil
  ): (List[Dataset], List[JsonLD]) = {
    val (datasets, jsons) = accumulator
    val newDatasets       = datasets :+ dataset
    val newJsons          = jsons ++ datasetJsons
    if (newDatasets.size < ifLessThan) {
      val newDataset = dataset.copy(
        id       = datasetIdentifiers.generateOne,
        sameAs   = dataset.entityId.asSameAs,
        projects = List(datasetProjects.generateOne)
      )
      val newDatasetCreatedDate = createdDate.shiftToFuture
      val newDatasetJsons       = newDataset.toJsonLD(newDatasetCreatedDate, noSameAs = false)
      nestDataset(ifLessThan)(newDataset, newDatasetCreatedDate, newDatasetJsons, newDatasets -> newJsons)
    } else newDatasets -> newJsons
  }

  private implicit class DateCreatedOps(dateCreated: DateCreated) {
    lazy val shiftToFuture = DateCreated(dateCreated.value plusSeconds positiveInts().generateOne.value)
  }

  private lazy val toPerson: DatasetCreator => Person =
    creator => Person(creator.name, creator.maybeEmail, creator.maybeAffiliation)

  private lazy val byName: Ordering[Dataset] =
    (ds1: Dataset, ds2: Dataset) => ds1.title.value compareTo ds2.title.value
}
