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
import ch.datascience.generators.Generators
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.graph.model.users.{Name => UserName}
import ch.datascience.http.rest.SortBy.Direction
import ch.datascience.http.rest.paging.PagingRequest
import ch.datascience.http.rest.paging.model.{Page, PerPage, Total}
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
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
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IODatasetsFinderSpec extends WordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks {

  protected override val givenServerRunning: Boolean = true

  "findDatasets - no phrase" should {

    Option(Phrase("*")) +: Option.empty[Phrase] +: Nil foreach { maybePhrase =>
      s"return all datasets when the given phrase is $maybePhrase - case of non-modified datasets" in new TestCase {

        val datasetsList = nonModifiedDatasets()
          .generateNonEmptyList(maxElements = Refined.unsafeApply(PagingRequest.default.perPage.value))
          .toList

        loadToStore(datasetsList flatMap (_.toJsonLDsAndProjects(noSameAs = false).jsonLDs): _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase, Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
          .unsafeRunSync()

        result.results shouldBe datasetsList
          .map(_.toDatasetSearchResult)
          .sortBy(_.name.value)

        result.pagingInfo.total shouldBe Total(datasetsList.size)
      }

      s"return latest versions of datasets when the given phrase is $maybePhrase - case of modified datasets" in new TestCase {

        val originalDatasetsList = nonModifiedDatasets(projects = nonEmptyList(datasetProjects, maxElements = 1))
          .generateNonEmptyList(maxElements = Refined.unsafeApply(PagingRequest.default.perPage.value))
          .toList
        val modifiedDatasetsList = originalDatasetsList.map { ds =>
          modifiedDatasets(ds, ds.projects.head, DerivedFrom(ds.entityId)).generateOne
            .copy(name = datasetNames.generateOne)
        }

        loadToStore(originalDatasetsList flatMap (_.toJsonLDsAndProjects(noSameAs = false).jsonLDs): _*)
        loadToStore(modifiedDatasetsList flatMap (_.toJsonLDs()): _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase, Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
          .unsafeRunSync()

        result.results shouldBe modifiedDatasetsList
          .map(_.toDatasetSearchResult)
          .sortBy(_.name.value)

        result.pagingInfo.total shouldBe Total(modifiedDatasetsList.size)
      }
    }

    "return the latest versions of modified datasets " +
      "if the older versions are not used in any project" in new TestCase {

      val originalDataset = nonModifiedDatasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne
      val datasetModification1 = modifiedDatasets(
        dataset             = originalDataset,
        project             = originalDataset.projects.head,
        derivedFromOverride = DerivedFrom(originalDataset.entityId)
      ).generateOne.copy(name = datasetNames.generateOne)
      val datasetModification2 = modifiedDatasets(
        dataset             = datasetModification1,
        project             = datasetModification1.projects.head,
        derivedFromOverride = DerivedFrom(datasetModification1.entityId)
      ).generateOne.copy(name = datasetNames.generateOne)

      loadToStore(
        List(
          originalDataset.toJsonLDsAndProjects(noSameAs = true).jsonLDs,
          datasetModification1.toJsonLDs(),
          datasetModification2.toJsonLDs()
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()

      result.results shouldBe List(datasetModification2.toDatasetSearchResult)
    }

    "return the latest versions of modified datasets together with some earlier versions " +
      "but only these earlier versions which are used in other projects and are not modified there" in new TestCase {

      val projects        = datasetProjects.generateNonEmptyList(minElements = 2, maxElements = 2)
      val originalDataset = nonModifiedDatasets(projects = Gen.const(projects)).generateOne
      val datasetModification = modifiedDatasets(
        dataset             = originalDataset,
        project             = originalDataset.projects.head,
        derivedFromOverride = DerivedFrom(originalDataset.entityId)
      ).generateOne.copy(name = datasetNames.generateOne)

      loadToStore(
        List(
          originalDataset.toJsonLDsAndProjects(noSameAs = true).jsonLDs,
          datasetModification.toJsonLDs()
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()

      result.results shouldBe List(
        originalDataset.copy(projects = projects.tail).toDatasetSearchResult,
        datasetModification.toDatasetSearchResult
      ).sortBy(_.name.value)
    }

    "return the latest versions of modified datasets " +
      "including the latest versions of datasets modified in other projects" in new TestCase {

      val projects                 = datasetProjects.generateNonEmptyList(minElements = 3, maxElements = 3)
      val originalDataset          = nonModifiedDatasets(projects = Gen.const(projects)).generateOne
      val originalJsonsAndProjects = originalDataset.toJsonLDsAndProjects(noSameAs = true)

      val (firstJson, firstProject) = originalJsonsAndProjects.head
      val datasetModificationOnProject1 = modifiedDatasets(
        originalDataset,
        firstProject,
        firstJson.entityId.map(DerivedFrom(_))
      ).generateOne.copy(name = datasetNames.generateOne)

      val (secondJson, secondProject) = originalJsonsAndProjects.tail.head
      val datasetModificationOnProject2 = modifiedDatasets(
        originalDataset,
        secondProject,
        secondJson.entityId.map(DerivedFrom(_))
      ).generateOne.copy(name = datasetNames.generateOne)

      loadToStore(
        List(
          originalJsonsAndProjects.jsonLDs,
          datasetModificationOnProject1.toJsonLDs(),
          datasetModificationOnProject2.toJsonLDs()
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()

      result.results shouldBe List(
        originalDataset
          .copy(projects = projects.tail.tail)
          .toDatasetSearchResult, // dataset in this version is still used here
        datasetModificationOnProject1.toDatasetSearchResult,
        datasetModificationOnProject2.toDatasetSearchResult
      ).sortBy(_.name.value)
    }

    "merge all datasets having the same sameAs pointing to external source" in new TestCase {
      val dataset1     = nonModifiedDatasets(projects = nonEmptyList(datasetProjects, minElements = 2)).generateOne
      val dataset2     = nonModifiedDatasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne
      val datasetsList = List(dataset1, dataset2)

      loadToStore(datasetsList flatMap (_.toJsonLDsAndProjects(noSameAs = false).jsonLDs): _*)

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest(Page.first, PerPage(2)))
        .unsafeRunSync()

      result.results shouldBe datasetsList
        .map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(datasetsList.size)
    }

    "merge datasets when they are imported from other renku project" in new TestCase {
      val dataset1     = nonModifiedDatasets(projects = nonEmptyList(datasetProjects, minElements = 2)).generateOne
      val dataset2     = nonModifiedDatasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne
      val datasetsList = List(dataset1, dataset2)

      loadToStore(datasetsList flatMap (_.toJsonLDsAndProjects(noSameAs = true).jsonLDs): _*)

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest(Page(1), PerPage(2)))
        .unsafeRunSync()

      result.results shouldBe datasetsList
        .map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(datasetsList.size)
    }

    "return all datasets having neither sameAs nor imported to and/or from other projects" in new TestCase {
      val dataset1 = nonModifiedDatasets(projects = nonEmptyList(datasetProjects, minElements = 2)).generateOne
      val dataset2 = nonModifiedDatasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne
      val dataset3 = nonModifiedDatasets(projects = nonEmptyList(datasetProjects, minElements = 2)).generateOne

      loadToStore(
        List(
          dataset1.toJsonLDsAndProjects(noSameAs = true).jsonLDs,
          dataset2.toJsonLDsAndProjects(noSameAs = true).jsonLDs,
          dataset3.toJsonLDsAndProjects(noSameAs = false).jsonLDs
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest(Page(1), PerPage(3)))
        .unsafeRunSync()

      val datasetsList = List(dataset1, dataset2, dataset3)
      result.results shouldBe datasetsList
        .map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(datasetsList.size)
    }
  }

  "findDatasets in case of forks - no phrase" should {

    "work when some datasets are modified on forks" in new TestCase {
      val origDs = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
      ).generateOne
      val (origDsProject1Json, origDsProject1) +: (origDsProject2Json, origDsProject2) +: Nil =
        origDs.toJsonLDsAndProjects(noSameAs = false)

      val modifiedOrigDsOnProject2 = modifiedDatasets(
        origDs,
        origDsProject2,
        derivedFromOverride = origDsProject2Json.entityId.map(DerivedFrom(_))
      ).generateOne

      val forkDs = origDs.copy(
        projects = datasetProjects.generateNonEmptyList(minElements = 2, maxElements = 2).toList
      )
      val (forkDsProject3Json, forkDsProject3) +: (forkDsProject4Json, forkDsProject4) +: Nil =
        forkDs.toJsonLDsAndProjects(noSameAs = false)

      val modifiedForkDsOnProject3 = modifiedDatasets(forkDs, forkDsProject3).generateOne

      loadToStore(
        List(origDsProject1Json, origDsProject2Json, forkDsProject3Json, forkDsProject4Json) ++
          modifiedOrigDsOnProject2.toJsonLDs() ++
          modifiedForkDsOnProject3.toJsonLDs(): _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()

      result.results should contain theSameElementsAs List(
        origDs.copy(projects                   = List(origDsProject1, forkDsProject4)).toDatasetSearchResult,
        modifiedOrigDsOnProject2.copy(projects = List(origDsProject2)).toDatasetSearchResult,
        modifiedForkDsOnProject3.copy(projects = List(forkDsProject3)).toDatasetSearchResult
      )

      result.pagingInfo.total shouldBe Total(3)
    }

    "merge all datasets having the same sameAs together with their forks to different projects" in new TestCase {
      val dataset1CreatedDate = datasetCreatedDates.generateOne
      val dataset1 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
      ).generateOne
      val dataset1Fork = dataset1.copy(projects = datasetProjects.generateList(ofSize = 1))

      val dataset2CreatedDate = dataset1CreatedDate.shiftToFuture
      val dataset2 = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        projects = datasetProjects.generateList(ofSize = 1)
      )
      val dataset2Fork = dataset2.copy(projects = datasetProjects.generateList(ofSize = 1))

      loadToStore(
        List(
          dataset1.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs     = false).jsonLDs,
          dataset1Fork.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs = false).jsonLDs,
          dataset2.toJsonLDsAndProjects(dataset2CreatedDate, noSameAs     = false).jsonLDs,
          dataset2Fork.toJsonLDsAndProjects(dataset2CreatedDate, noSameAs = false).jsonLDs
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest(Page.first, PerPage(1)))
        .unsafeRunSync()

      result.results shouldBe List(
        dataset1 addAll dataset1Fork.projects addAll dataset2.projects addAll dataset2Fork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(1)
    }

    "merge datasets imported from other renku project and forked to different projects" in new TestCase {
      val initialDatasetCreatedDate = datasetCreatedDates.generateOne
      val initialDataset = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 3, maxElements = 3)
      ).generateOne
      val initialDatasetFork = initialDataset.copy(projects = datasetProjects.generateList(ofSize = 1))

      val importedDatasetCreatedDate = initialDatasetCreatedDate.shiftToFuture
      val importedDataset = initialDataset.copy(
        id       = datasetIdentifiers.generateOne,
        projects = datasetProjects.generateList(ofSize = 1),
        sameAs   = initialDataset.entityId.asSameAs
      )
      val importedDatasetFork = importedDataset.copy(projects = datasetProjects.generateList(ofSize = 1))

      loadToStore(
        List(
          initialDataset.toJsonLDsAndProjects(initialDatasetCreatedDate, noSameAs       = true).jsonLDs,
          initialDatasetFork.toJsonLDsAndProjects(initialDatasetCreatedDate, noSameAs   = true).jsonLDs,
          importedDataset.toJsonLDsAndProjects(importedDatasetCreatedDate, noSameAs     = false).jsonLDs,
          importedDatasetFork.toJsonLDsAndProjects(importedDatasetCreatedDate, noSameAs = false).jsonLDs
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest(Page.first, PerPage(1)))
        .unsafeRunSync()

      result.results shouldBe List(
        initialDataset addAll initialDatasetFork.projects addAll importedDataset.projects addAll importedDatasetFork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(1)
    }

    "merge non-imported and not being imported datasets when they are forked to different projects" in new TestCase {
      val dataset1 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne
      val dataset1Fork = dataset1.copy(projects = datasetProjects.generateList(ofSize = 1))

      val dataset2 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne
      val dataset2Fork = dataset2.copy(projects = datasetProjects.generateList(ofSize = 1))

      loadToStore(
        List(
          dataset1.toJsonLDsAndProjects(noSameAs     = true).jsonLDs,
          dataset1Fork.toJsonLDsAndProjects(noSameAs = true).jsonLDs,
          dataset2.toJsonLDsAndProjects(noSameAs     = true).jsonLDs,
          dataset2Fork.toJsonLDsAndProjects(noSameAs = true).jsonLDs
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = None, Sort.By(NameProperty, Direction.Asc), PagingRequest(Page.first, PerPage(2)))
        .unsafeRunSync()

      result.results shouldBe List(
        dataset1 addAll dataset1Fork.projects,
        dataset2 addAll dataset2Fork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(2)
    }
  }

  "findDatasets - some phrase given" should {

    "merge all datasets having the same sameAs pointing to some external storage" in new TestCase {

      val phrase = phrases.generateOne
      val dataset1 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset2 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeDescContaining(phrase)
      val dataset3 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeCreatorNameContaining(phrase)

      loadToStore(
        List(dataset1, dataset2, dataset3, nonModifiedDatasets().generateOne) flatMap (_.toJsonLDsAndProjects(
          noSameAs = false
        ).jsonLDs): _*
      )

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(NameProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      val matchingDatasets = List(dataset1, dataset2, dataset3)
      result.results shouldBe List(matchingDatasets.sorted(byName)(1).toDatasetSearchResult)

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    "merge datasets when they are imported from other renku project" in new TestCase {

      val phrase = phrases.generateOne
      val dataset1 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset2 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeDescContaining(phrase)
      val dataset3 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeCreatorNameContaining(phrase)

      loadToStore(
        List(dataset1, dataset2, dataset3, nonModifiedDatasets().generateOne) flatMap (_.toJsonLDsAndProjects(
          noSameAs = true
        ).jsonLDs): _*
      )

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(NameProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      val matchingDatasets = List(dataset1, dataset2, dataset3)
      result.results shouldBe List((matchingDatasets sorted byName)(1).toDatasetSearchResult)

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    "return datasets having neither sameAs nor imported to other projects" in new TestCase {

      val phrase = phrases.generateOne
      val dataset1 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset2 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeDescContaining(phrase)
      val dataset3 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeCreatorNameContaining(phrase)

      loadToStore(
        List(
          dataset1.toJsonLDsAndProjects(noSameAs                          = true).jsonLDs,
          dataset2.toJsonLDsAndProjects(noSameAs                          = true).jsonLDs,
          dataset3.toJsonLDsAndProjects(noSameAs                          = false).jsonLDs,
          nonModifiedDatasets().generateOne.toJsonLDsAndProjects(noSameAs = false).jsonLDs
        ).flatten: _*
      )

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(NameProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      val matchingDatasets = List(dataset1, dataset2, dataset3)
      result.results shouldBe List(matchingDatasets.sorted(byName)(1).toDatasetSearchResult)

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    "return no results if there is no matching dataset" in new TestCase {

      val dataset = nonModifiedDatasets().generateOne

      loadToStore(dataset.toJsonLDsAndProjects(noSameAs = true).jsonLDs: _*)

      val result = datasetsFinder
        .findDatasets(Some(phrases.generateOne), Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()

      result.results          shouldBe empty
      result.pagingInfo.total shouldBe Total(0)
    }

    "not return datasets if the match was only in an older version which is not used anymore" in new TestCase {

      val project  = datasetProjects.generateOne
      val phrase   = phrases.generateOne
      val original = nonModifiedDatasets().generateOne.copy(projects = List(project)).makeNameContaining(phrase)
      val fork     = modifiedDatasets(original, project).generateOne.copy(name = datasetNames.generateOne)

      loadToStore(
        List(
          original.toJsonLDsAndProjects(noSameAs = true).jsonLDs,
          fork.toJsonLDs()
        ).flatten: _*
      )

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()
        .results shouldBe empty
    }

    "not return datasets neither sharing the sameAs not in the derivation hierarchy " +
      "if they do not match the criteria" in new TestCase {

      val dataset1 = nonModifiedDatasets().generateOne.copy()
      val phrase   = phrases.generateOne
      val project  = datasetProjects.generateOne
      val dataset2 = nonModifiedDatasets().generateOne.copy(projects = List(project)).makeNameContaining(phrase)

      loadToStore(List(dataset1, dataset2).flatMap(_.toJsonLDsAndProjects(noSameAs = true)).jsonLDs: _*)

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(NameProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()
        .results shouldBe List(dataset1.toDatasetSearchResult)
    }
  }

  "findDatasets in case of forks - some phrase given" should {

    "merge all datasets having the same sameAs together with forks to different projects" in new TestCase {
      val phrase = phrases.generateOne

      val dataset1CreatedDate = datasetCreatedDates.generateOne
      val dataset1 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset1Fork            = dataset1.copy(projects = datasetProjects.generateList(ofSize = 1))
      val dataset1PrimCreatedDate = dataset1CreatedDate.shiftToFuture
      val dataset1Prim = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        projects = datasetProjects.generateList(ofSize = 1)
      )
      val dataset1PrimFork    = dataset1Prim.copy(projects = datasetProjects.generateList(ofSize = 1))
      val dataset2CreatedDate = datasetCreatedDates.generateOne
      val dataset2 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeDescContaining(phrase)
      val dataset2Fork        = dataset2.copy(projects = datasetProjects.generateList(ofSize = 1))
      val dataset3CreatedDate = datasetCreatedDates.generateOne
      val dataset3 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeCreatorNameContaining(phrase)
      val dataset3Fork = dataset3.copy(projects = datasetProjects.generateList(ofSize = 1))

      loadToStore(
        List(
          dataset1.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs             = false).jsonLDs,
          dataset1Fork.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs         = false).jsonLDs,
          dataset1Prim.toJsonLDsAndProjects(dataset1PrimCreatedDate, noSameAs     = false).jsonLDs,
          dataset1PrimFork.toJsonLDsAndProjects(dataset1PrimCreatedDate, noSameAs = false).jsonLDs,
          dataset2.toJsonLDsAndProjects(dataset2CreatedDate, noSameAs             = false).jsonLDs,
          dataset2Fork.toJsonLDsAndProjects(dataset2CreatedDate, noSameAs         = false).jsonLDs,
          dataset3.toJsonLDsAndProjects(dataset3CreatedDate, noSameAs             = false).jsonLDs,
          dataset3Fork.toJsonLDsAndProjects(dataset3CreatedDate, noSameAs         = false).jsonLDs
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = Some(phrase),
                      Sort.By(NameProperty, Direction.Asc),
                      PagingRequest(Page(1), PerPage(3)))
        .unsafeRunSync()

      result.results shouldBe List(
        dataset1 addAll dataset1Fork.projects addAll dataset1Prim.projects addAll dataset1PrimFork.projects,
        dataset2 addAll dataset2Fork.projects,
        dataset3 addAll dataset3Fork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(3)
    }

    "merge datasets imported from other renku project and forked to different projects" in new TestCase {
      val phrase = phrases.generateOne

      val dataset1CreatedDate = datasetCreatedDates.generateOne
      val dataset1 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeNameContaining(phrase)
      val dataset1Fork            = dataset1.copy(projects = datasetProjects.generateList(ofSize = 1))
      val dataset1PrimCreatedDate = dataset1CreatedDate.shiftToFuture
      val dataset1Prim = dataset1.copy(
        id       = datasetIdentifiers.generateOne,
        projects = datasetProjects.generateList(ofSize = 1),
        sameAs   = dataset1.entityId.asSameAs
      )
      val dataset1PrimFork    = dataset1Prim.copy(projects = datasetProjects.generateList(ofSize = 1))
      val dataset2CreatedDate = datasetCreatedDates.generateOne
      val dataset2 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeDescContaining(phrase)
      val dataset2Fork        = dataset2.copy(projects = datasetProjects.generateList(ofSize = 1))
      val dataset3CreatedDate = datasetCreatedDates.generateOne
      val dataset3 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, minElements = 2)
      ).generateOne.makeCreatorNameContaining(phrase)
      val dataset3Fork = dataset3.copy(projects = datasetProjects.generateList(ofSize = 1))

      loadToStore(
        List(
          dataset1.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs             = true).jsonLDs,
          dataset1Fork.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs         = true).jsonLDs,
          dataset1Prim.toJsonLDsAndProjects(dataset1PrimCreatedDate, noSameAs     = false).jsonLDs,
          dataset1PrimFork.toJsonLDsAndProjects(dataset1PrimCreatedDate, noSameAs = false).jsonLDs,
          dataset2.toJsonLDsAndProjects(dataset2CreatedDate, noSameAs             = true).jsonLDs,
          dataset2Fork.toJsonLDsAndProjects(dataset2CreatedDate, noSameAs         = true).jsonLDs,
          dataset3.toJsonLDsAndProjects(dataset3CreatedDate, noSameAs             = true).jsonLDs,
          dataset3Fork.toJsonLDsAndProjects(dataset3CreatedDate, noSameAs         = true).jsonLDs
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = Some(phrase),
                      Sort.By(NameProperty, Direction.Asc),
                      PagingRequest(Page(1), PerPage(3)))
        .unsafeRunSync()

      result.results shouldBe List(
        dataset1 addAll dataset1Fork.projects addAll dataset1Prim.projects addAll dataset1PrimFork.projects,
        dataset2 addAll dataset2Fork.projects,
        dataset3 addAll dataset3Fork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(3)
    }

    "merge non-imported and not being imported datasets when they are forked to different projects" in new TestCase {
      val phrase = phrases.generateOne

      val dataset1CreatedDate = datasetCreatedDates.generateOne
      val dataset1 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeNameContaining(phrase)
      val dataset1Fork = dataset1.copy(projects = datasetProjects.generateList(ofSize = 1))

      val dataset2CreatedDate = datasetCreatedDates.generateOne
      val dataset2 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeDescContaining(phrase)
      val dataset2Fork = dataset2.copy(projects = datasetProjects.generateList(ofSize = 1))

      val dataset3CreatedDate = datasetCreatedDates.generateOne
      val dataset3 = nonModifiedDatasets(
        projects = nonEmptyList(datasetProjects, maxElements = 1)
      ).generateOne.makeCreatorNameContaining(phrase)
      val dataset3Fork = dataset3.copy(projects = datasetProjects.generateList(ofSize = 1))

      loadToStore(
        List(
          dataset1.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs     = true).jsonLDs,
          dataset1Fork.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs = true).jsonLDs,
          dataset2.toJsonLDsAndProjects(dataset2CreatedDate, noSameAs     = true).jsonLDs,
          dataset2Fork.toJsonLDsAndProjects(dataset2CreatedDate, noSameAs = true).jsonLDs,
          dataset3.toJsonLDsAndProjects(dataset3CreatedDate, noSameAs     = true).jsonLDs,
          dataset3Fork.toJsonLDsAndProjects(dataset3CreatedDate, noSameAs = true).jsonLDs
        ).flatten: _*
      )

      val result = datasetsFinder
        .findDatasets(maybePhrase = Some(phrase),
                      Sort.By(NameProperty, Direction.Asc),
                      PagingRequest(Page(1), PerPage(3)))
        .unsafeRunSync()

      result.results shouldBe List(
        dataset1 addAll dataset1Fork.projects,
        dataset2 addAll dataset2Fork.projects,
        dataset3 addAll dataset3Fork.projects
      ).map(_.toDatasetSearchResult)
        .sortBy(_.name.value)

      result.pagingInfo.total shouldBe Total(3)
    }
  }

  "findDatasets with explicit sorting given" should {

    s"return datasets with name, description or creator matching the given phrase sorted by $NameProperty" in new TestCase {
      forAll(nonModifiedDatasets(), nonModifiedDatasets(), nonModifiedDatasets(), nonModifiedDatasets()) {
        (dataset1Orig, dataset2Orig, dataset3Orig, nonPhrased) =>
          val phrase                         = phrases.generateOne
          val (dataset1, dataset2, dataset3) = addPhrase(phrase, dataset1Orig, dataset2Orig, dataset3Orig)

          loadToStore(
            List(dataset1, dataset2, dataset3, nonPhrased) flatMap (_.toJsonLDsAndProjects(noSameAs = false).jsonLDs): _*
          )

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
        nonModifiedDatasets().generateOne changePublishedDateTo Some(PublishedDate(LocalDate.now() minusDays 1)),
        nonModifiedDatasets().generateOne changePublishedDateTo None,
        nonModifiedDatasets().generateOne changePublishedDateTo Some(PublishedDate(LocalDate.now()))
      )

      loadToStore(
        List(dataset1, dataset2, dataset3, nonModifiedDatasets().generateOne) flatMap (_.toJsonLDsAndProjects(
          noSameAs = false
        ).jsonLDs): _*
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
        nonModifiedDatasets(projects = nonEmptyList(datasetProjects, minElements = 4, maxElements = 4)).generateOne,
        nonModifiedDatasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne,
        nonModifiedDatasets(projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)).generateOne
      )

      loadToStore(
        List(dataset1, dataset2, dataset3, nonModifiedDatasets().generateOne) flatMap (_.toJsonLDsAndProjects(
          noSameAs = false
        ).jsonLDs): _*
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
        addPhrase(phrase,
                  nonModifiedDatasets().generateOne,
                  nonModifiedDatasets().generateOne,
                  nonModifiedDatasets().generateOne)

      loadToStore(
        List(dataset1, dataset2, dataset3, nonModifiedDatasets().generateOne) flatMap (_.toJsonLDsAndProjects(
          noSameAs = false
        ).jsonLDs): _*
      )

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
        addPhrase(phrase,
                  nonModifiedDatasets().generateOne,
                  nonModifiedDatasets().generateOne,
                  nonModifiedDatasets().generateOne)

      loadToStore(
        List(dataset1, dataset2, dataset3, nonModifiedDatasets().generateOne) flatMap (_.toJsonLDsAndProjects(
          noSameAs = false
        ).jsonLDs): _*
      )

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

  "findDatasets in the case there's datasets import hierarchy" should {

    emptyOptionOf[Phrase] +: phrases.toGeneratorOfSomes +: Nil foreach { phraseGenerator =>
      val maybePhrase = phraseGenerator.generateOne

      s"return a single dataset - case when the first dataset is externally imported and ${maybePhrase.getOrElse("no")} phrase given" in new TestCase {
        val dataset1 = nonModifiedDatasets(
          projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
        ).generateOne.makeDescContaining(maybePhrase)
        val dataset1CreatedDate = datasetCreatedDates.generateOne
        val dataset1Jsons       = dataset1.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs = false).jsonLDs

        val dataset2 = dataset1.copy(
          id       = datasetIdentifiers.generateOne,
          sameAs   = dataset1.entityId.asSameAs,
          projects = datasetProjects.generateList(ofSize = 1)
        )
        val dataset2CreatedDate = dataset1CreatedDate.shiftToFuture
        val dataset2Jsons       = dataset2.toJsonLDsAndProjects(dataset2CreatedDate, noSameAs = false).jsonLDs

        val dataset3 = dataset2.copy(
          id       = datasetIdentifiers.generateOne,
          sameAs   = dataset2.entityId.asSameAs,
          projects = datasetProjects.generateList(ofSize = 1)
        )
        val dataset3Jsons = dataset3.toJsonLDsAndProjects(dataset2CreatedDate.shiftToFuture, noSameAs = false).jsonLDs

        loadToStore(dataset1Jsons ++ dataset2Jsons ++ dataset3Jsons: _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase, Sort.By(NameProperty, Direction.Asc), PagingRequest(Page(1), PerPage(1)))
          .unsafeRunSync()

        result.results shouldBe List(
          dataset1 addAll dataset2.projects addAll dataset3.projects
        ).map(_.toDatasetSearchResult)
          .sortBy(_.name.value)

        result.pagingInfo.total shouldBe Total(1)
      }

      s"return a single dataset - case when the first dataset is in-project created and ${maybePhrase.getOrElse("no")} phrase given" in new TestCase {
        val dataset1 = nonModifiedDatasets(
          projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
        ).generateOne.makeDescContaining(maybePhrase)
        val dataset1CreatedDate = datasetCreatedDates.generateOne
        val dataset1Jsons       = dataset1.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs = true).jsonLDs

        val dataset2 = dataset1.copy(
          id       = datasetIdentifiers.generateOne,
          sameAs   = dataset1.entityId.asSameAs,
          projects = datasetProjects.generateList(ofSize = 1)
        )
        val dataset2CreatedDate = dataset1CreatedDate.shiftToFuture
        val dataset2Jsons       = dataset2.toJsonLDsAndProjects(dataset2CreatedDate, noSameAs = false).jsonLDs

        val dataset3 = dataset2.copy(
          id       = datasetIdentifiers.generateOne,
          sameAs   = dataset2.entityId.asSameAs,
          projects = datasetProjects.generateList(ofSize = 1)
        )
        val dataset3Jsons = dataset3.toJsonLDsAndProjects(dataset2CreatedDate.shiftToFuture, noSameAs = false).jsonLDs

        loadToStore(dataset1Jsons ++ dataset2Jsons ++ dataset3Jsons: _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase, Sort.By(NameProperty, Direction.Asc), PagingRequest(Page(1), PerPage(1)))
          .unsafeRunSync()

        result.results shouldBe List(
          dataset1 addAll dataset2.projects addAll dataset3.projects
        ).map(_.toDatasetSearchResult)
          .sortBy(_.name.value)

        result.pagingInfo.total shouldBe Total(1)
      }

      "return a single dataset - case when there're two first level projects sharing a dataset " +
        s"and some other project imports from on of these two; ${maybePhrase.getOrElse("no")} phrase given" in new TestCase {
        val dataset1 = nonModifiedDatasets(
          projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
        ).generateOne.makeDescContaining(maybePhrase)
        val dataset1CreatedDate = datasetCreatedDates.generateOne
        val dataset1Jsons       = dataset1.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs = false).jsonLDs

        val dataset2 = nonModifiedDatasets(
          sameAs   = Gen.const(dataset1.sameAs),
          projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
        ).generateOne
        val dataset2CreatedDate = dataset1CreatedDate.shiftToFuture
        val dataset2Jsons       = dataset2.toJsonLDsAndProjects(dataset2CreatedDate, noSameAs = false).jsonLDs

        val dataset3 = dataset2.copy(
          id       = datasetIdentifiers.generateOne,
          sameAs   = dataset2.entityId.asSameAs,
          projects = datasetProjects.generateList(ofSize = 1)
        )
        val dataset3Jsons = dataset3.toJsonLDsAndProjects(dataset2CreatedDate.shiftToFuture, noSameAs = false).jsonLDs

        loadToStore(dataset1Jsons ++ dataset2Jsons ++ dataset3Jsons: _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase, Sort.By(NameProperty, Direction.Asc), PagingRequest(Page(1), PerPage(1)))
          .unsafeRunSync()

        result.results shouldBe List(
          dataset1 addAll dataset2.projects addAll dataset3.projects
        ).map(_.toDatasetSearchResult)
          .sortBy(_.name.value)

        result.pagingInfo.total shouldBe Total(1)
      }

      "return a single dataset " +
        s"- case when there are 4 levels of inheritance and ${maybePhrase.getOrElse("no")} phrase given" in new TestCase {
        val dataset1 = nonModifiedDatasets(
          projects = nonEmptyList(datasetProjects, maxElements = 3)
        ).generateOne.makeDescContaining(maybePhrase)
        val dataset1CreatedDate = datasetCreatedDates.generateOne
        val dataset1Jsons       = dataset1.toJsonLDsAndProjects(dataset1CreatedDate, noSameAs = false).jsonLDs

        val (allDatasets, allJsons) = nestDataset(ifLessThan = 4)(dataset1, dataset1CreatedDate, dataset1Jsons)

        loadToStore(allJsons: _*)

        val result = datasetsFinder
          .findDatasets(maybePhrase, Sort.By(NameProperty, Direction.Asc), PagingRequest(Page(1), PerPage(1)))
          .unsafeRunSync()

        result.results shouldBe List(
          dataset1 addAll allDatasets.tail.flatMap(_.projects)
        ).map(_.toDatasetSearchResult)
          .sortBy(_.name.value)

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
      dataset1Orig:      NonModifiedDataset,
      dataset2Orig:      NonModifiedDataset,
      dataset3Orig:      NonModifiedDataset
  ): (NonModifiedDataset, NonModifiedDataset, NonModifiedDataset) = {
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
            userEmails.generateOption,
            sentenceContaining(nonEmptyPhrase).map(_.value).map(UserName.apply).generateOne,
            userAffiliations.generateOption
          )
        )
      )
    )

    (dataset1, dataset2, dataset3)
  }

  private implicit class NonModifiedDatasetOps(dataset: NonModifiedDataset) {

    lazy val entityId: EntityId = DataSet.entityId(dataset.id)

    def changePublishedDateTo(maybeDate: Option[PublishedDate]): NonModifiedDataset =
      dataset.copy(published = dataset.published.copy(maybeDate = maybeDate))

    def addAll(projects: List[DatasetProject]): NonModifiedDataset =
      dataset.copy(projects = dataset.projects ++ projects)

    def makeNameContaining(phrase: Phrase): NonModifiedDataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataset.copy(
        name = sentenceContaining(nonEmptyPhrase).map(_.value).map(Name.apply).generateOne
      )
    }

    def makeCreatorNameContaining(phrase: Phrase): NonModifiedDataset = {
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

    def makeDescContaining(maybePhrase: Option[Phrase]): NonModifiedDataset =
      maybePhrase map makeDescContaining getOrElse dataset

    def makeDescContaining(phrase: Phrase): NonModifiedDataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataset.copy(
        maybeDescription = sentenceContaining(nonEmptyPhrase).map(_.value).map(Description.apply).generateSome
      )
    }

    lazy val toDatasetSearchResult: DatasetSearchResult = DatasetSearchResult(
      dataset.id,
      dataset.name,
      dataset.maybeDescription,
      dataset.published,
      ProjectsCount(dataset.projects.size)
    )

    def toJsonLDsAndProjects(noSameAs: Boolean): List[(JsonLD, DatasetProject)] = toJsonLDsAndProjects(
      firstDatasetDateCreated = DateCreated(dataset.projects.map(_.created.date.value).min),
      noSameAs                = noSameAs
    )

    def toJsonLDsAndProjects(firstDatasetDateCreated: DateCreated, noSameAs: Boolean): List[(JsonLD, DatasetProject)] =
      dataset.projects match {
        case firstProject +: otherProjects =>
          val firstTuple @ (firstJsonLd, _) = nonModifiedDataSetCommit(
            committedDate = CommittedDate(firstDatasetDateCreated.value)
          )(
            projectPath = firstProject.path
          )(
            datasetIdentifier         = dataset.id,
            datasetName               = dataset.name,
            datasetUrl                = dataset.url,
            maybeDatasetSameAs        = if (noSameAs) None else dataset.sameAs.some,
            maybeDatasetDescription   = dataset.maybeDescription,
            maybeDatasetPublishedDate = dataset.published.maybeDate,
            datasetCreatedDate        = firstDatasetDateCreated,
            datasetCreators           = dataset.published.creators map toPerson
          ) -> firstProject

          val someSameAs =
            if (noSameAs) DataSet.entityId(dataset.id).asSameAs.some
            else dataset.sameAs.some
          val otherTuples = otherProjects.map { project =>
            val projectDateCreated = firstDatasetDateCreated.shiftToFuture
            nonModifiedDataSetCommit(
              committedDate = CommittedDate(projectDateCreated.value)
            )(
              projectPath = project.path
            )(
              datasetName               = dataset.name,
              datasetUrl                = dataset.url,
              maybeDatasetSameAs        = someSameAs,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreatedDate        = projectDateCreated,
              datasetCreators           = dataset.published.creators map toPerson
            ) -> project
          }

          firstTuple +: otherTuples
      }
  }

  private implicit class ModifiedDatasetOps(dataset: ModifiedDataset) {

    lazy val entityId: EntityId = DataSet.entityId(dataset.id)

    lazy val toDatasetSearchResult: DatasetSearchResult = DatasetSearchResult(
      dataset.id,
      dataset.name,
      dataset.maybeDescription,
      dataset.published,
      ProjectsCount(dataset.projects.size)
    )

    def toJsonLDs(
        firstDatasetDateCreated: DateCreated = DateCreated(dataset.projects.map(_.created.date.value).min)
    ): List[JsonLD] =
      dataset.projects match {
        case firstProject +: otherProjects =>
          val firstJsonLd = modifiedDataSetCommit(
            committedDate = CommittedDate(firstDatasetDateCreated.value)
          )(
            projectPath = firstProject.path
          )(
            datasetIdentifier         = dataset.id,
            datasetName               = dataset.name,
            datasetUrl                = dataset.url,
            datasetDerivedFrom        = dataset.derivedFrom,
            maybeDatasetDescription   = dataset.maybeDescription,
            maybeDatasetPublishedDate = dataset.published.maybeDate,
            datasetCreatedDate        = DateCreated(firstDatasetDateCreated.value),
            datasetCreators           = dataset.published.creators map toPerson
          )

          val otherJsonLds = otherProjects.map { project =>
            val projectDateCreated = datasetInProjectCreationDates generateGreaterThan DateCreatedInProject(
              firstDatasetDateCreated.value
            )

            modifiedDataSetCommit(
              committedDate = CommittedDate(projectDateCreated.value)
            )(
              projectPath = project.path
            )(
              datasetName               = dataset.name,
              datasetDerivedFrom        = dataset.derivedFrom,
              datasetUrl                = dataset.url,
              maybeDatasetDescription   = dataset.maybeDescription,
              maybeDatasetPublishedDate = dataset.published.maybeDate,
              datasetCreatedDate        = DateCreated(projectDateCreated.value),
              datasetCreators           = dataset.published.creators map toPerson
            )
          }

          firstJsonLd +: otherJsonLds
      }
  }

  private implicit class JsonAndProjectTuplesOps(tuples: List[(JsonLD, DatasetProject)]) {
    lazy val jsonLDs: List[JsonLD] = tuples.map(_._1)
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
      dataset:                        NonModifiedDataset,
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
        projects = datasetProjects.generateList(ofSize = 1)
      )
      val newDatasetCreatedDate = createdDate.shiftToFuture
      val newDatasetJsons       = newDataset.toJsonLDsAndProjects(newDatasetCreatedDate, noSameAs = false).jsonLDs
      nestDataset(ifLessThan)(newDataset, newDatasetCreatedDate, newDatasetJsons, newDatasets -> newJsons)
    } else newDatasets -> newJsons
  }

  private implicit class DateCreatedOps(dateCreated: DateCreated) {
    lazy val shiftToFuture = DateCreated(dateCreated.value plusSeconds positiveInts().generateOne.value)
  }

  private lazy val toPerson: DatasetCreator => Person =
    creator => Person(creator.name, creator.maybeEmail, creator.maybeAffiliation)

  private lazy val byName: Ordering[Dataset] =
    (ds1: Dataset, ds2: Dataset) => ds1.name.value compareTo ds2.name.value

  private implicit val dateCreatedInProjectOrdering: Ordering[DateCreatedInProject] =
    (x: DateCreatedInProject, y: DateCreatedInProject) => x.value compareTo y.value
}
