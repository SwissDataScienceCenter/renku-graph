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

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.RenkuBaseUrl
import ch.datascience.graph.model.projects.{ForksCount, Visibility}
import ch.datascience.graph.model.testentities.EntitiesGenerators._
import ch.datascience.graph.model.testentities.{Dataset, Person}
import ch.datascience.http.rest.SortBy.Direction
import ch.datascience.http.rest.paging.PagingRequest
import ch.datascience.http.rest.paging.model.{Page, PerPage, Total}
import ch.datascience.http.server.security.model.AuthUser
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.datasets.model.DatasetCreator
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.{DatasetSearchResult, ProjectsCount}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import eu.timepit.refined.api.Refined
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatasetsFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "findDatasets - no phrase" should {

    Option(Phrase("*")) :: Option.empty[Phrase] :: Nil foreach { maybePhrase =>
      s"return all datasets when the given phrase is $maybePhrase " +
        "- case of datasets that has neither sameAs nor are imported to and/or from other projects" in new TestCase {
          val dataset1 = datasetEntities(datasetProvenanceInternal).generateOne
          val dataset2Imported =
            dataset1 importTo projectEntities[ForksCount.Zero](visibilityPublic).generateOne

          val dataset3 = datasetEntities(datasetProvenanceInternal).generateOne

          loadToStore(dataset1, dataset2Imported, dataset3)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          val expectedResults = List(
            List(dataset1, dataset2Imported).toDatasetSearchResult(matchIdFrom = result.results),
            List(dataset3.toDatasetSearchResult(projectsCount = 1))
          ).flatten.sortBy(_.title)

          result.results shouldBe expectedResults

          result.pagingInfo.total shouldBe Total(expectedResults.size)
        }

      s"return all datasets when the given phrase is $maybePhrase " +
        "- case of non-modified datasets" in new TestCase {

          val datasets = datasetEntities(provenanceGen = datasetProvenanceImportedExternal)
            .generateNonEmptyList(maxElements = Refined.unsafeApply(PagingRequest.default.perPage.value))
            .toList

          loadToStore(datasets: _*)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results shouldBe datasets
            .map(_.toDatasetSearchResult(projectsCount = 1))
            .sortBy(_.title)

          result.pagingInfo.total shouldBe Total(datasets.size)
        }

      s"return all datasets when the given phrase is $maybePhrase " +
        "- case of shared sameAs" in new TestCase {

          val datasets = importedExternalDatasetEntities(sharedInProjects = 3).generateOne

          loadToStore(datasets: _*)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results shouldBe datasets.toDatasetSearchResult(matchIdFrom = result.results).toList

          result.pagingInfo.total shouldBe Total(1)
        }

      s"return all datasets when the given phrase is $maybePhrase " +
        "- case of shared sameAs with modification on some projects" in new TestCase {

          val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(sharedInProjects = 2).generateOne

          val dataset2Modified = modifiedDatasetEntities(dataset2).generateOne

          loadToStore(dataset1, dataset2, dataset2Modified)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results shouldBe List(dataset1, dataset2Modified)
            .map(_.toDatasetSearchResult(projectsCount = 1))
            .sortBy(_.title)

          result.pagingInfo.total shouldBe Total(2)
        }

      s"return all datasets when the given phrase is $maybePhrase " +
        "- case of shared sameAs and forks" in new TestCase {

          val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(sharedInProjects = 2).generateOne

          val datasets2Fork = dataset2.forkProject().fork

          loadToStore(dataset1, dataset2, datasets2Fork)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results shouldBe List(dataset1, dataset2, datasets2Fork)
            .toDatasetSearchResult(matchIdFrom = result.results)
            .toList

          result.pagingInfo.total shouldBe Total(1)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case of one level of modification" in new TestCase {

          val originalDatasetsList = datasetEntities(datasetProvenanceImportedExternal)
            .generateNonEmptyList(maxElements = Refined.unsafeApply(PagingRequest.default.perPage.value))
            .toList
          val modifiedDatasetsList = originalDatasetsList.map(modifiedDatasetEntities(_).generateOne)

          loadToStore(originalDatasetsList ++ modifiedDatasetsList: _*)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results shouldBe modifiedDatasetsList
            .map(_.toDatasetSearchResult(projectsCount = 1))
            .sortBy(_.title)

          result.pagingInfo.total shouldBe Total(modifiedDatasetsList.size)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case more than one level of modification" in new TestCase {

          val original      = datasetEntities(datasetProvenanceInternal).generateOne
          val modification1 = modifiedDatasetEntities(original).generateOne
          val modification2 = modifiedDatasetEntities(modification1).generateOne

          loadToStore(original, modification1, modification2)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results            should contain only modification2.toDatasetSearchResult(projectsCount = 1)
          result.pagingInfo.total shouldBe Total(1)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case if there are modified and non-modified datasets" in new TestCase {

          val dataset1             = datasetEntities(datasetProvenanceInternal).generateOne
          val dataset1Modification = modifiedDatasetEntities(dataset1).generateOne
          val dataset2             = datasetEntities(datasetProvenanceImportedExternal).generateOne

          loadToStore(dataset1, dataset1Modification, dataset2)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results shouldBe List(dataset1Modification, dataset2)
            .map(_.toDatasetSearchResult(projectsCount = 1))
            .sortBy(_.title)
          result.pagingInfo.total shouldBe Total(2)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case if shared datasets are modified on some projects but not all" in new TestCase {

          val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(2).generateOne
          val dataset2Modified            = modifiedDatasetEntities(dataset2).generateOne

          loadToStore(dataset1, dataset2, dataset2Modified)

          val result = datasetsFinder
            .findDatasets(None, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results shouldBe List(dataset1, dataset2Modified)
            .map(_.toDatasetSearchResult(projectsCount = 1))
            .sortBy(_.title)

          result.pagingInfo.total shouldBe Total(2)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case with forks on renku created datasets" in new TestCase {

          val dataset     = datasetEntities(datasetProvenanceInternal).generateOne
          val datasetFork = dataset.forkProject().fork

          loadToStore(dataset, datasetFork)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results should contain theSameElementsAs List(dataset, datasetFork)
            .toDatasetSearchResult(matchIdFrom = result.results)
            .toList

          result.pagingInfo.total shouldBe Total(1)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case with more than one level of modification and forks on the 1st level" in new TestCase {

          val dataset             = datasetEntities(datasetProvenanceInternal).generateOne
          val datasetFork         = dataset.forkProject().fork
          val datasetModification = modifiedDatasetEntities(dataset).generateOne

          loadToStore(dataset, datasetFork, datasetModification)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results shouldBe List(
            datasetFork.toDatasetSearchResult(projectsCount = 1),
            datasetModification.toDatasetSearchResult(projectsCount = 1)
          ).sortBy(_.title)

          result.pagingInfo.total shouldBe Total(2)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case with more than one level of modification and forks on not the 1st level" in new TestCase {

          val dataset                   = datasetEntities(datasetProvenanceImportedExternal).generateOne
          val datasetModification       = modifiedDatasetEntities(dataset).generateOne
          val datasetModificationFork   = datasetModification.forkProject().fork
          val datasetModificationOnFork = modifiedDatasetEntities(datasetModificationFork).generateOne

          loadToStore(dataset, datasetModification, datasetModificationFork, datasetModificationOnFork)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results should contain theSameElementsAs List(
            datasetModification.toDatasetSearchResult(projectsCount = 1),
            datasetModificationOnFork.toDatasetSearchResult(projectsCount = 1)
          ).sortBy(_.title)

          result.pagingInfo.total shouldBe Total(2)
        }

      s"not return deleted datasets when the given phrase is $maybePhrase" +
        "- case with unrelated datasets" in new TestCase {
          val dataset1 = datasetEntities(datasetProvenanceImportedExternal).generateOne
          val dataset2 = datasetEntities(datasetProvenanceImportedExternal).generateOne
          val dataset2Deleted = dataset2
            .invalidate(
              invalidationTimes(dataset2.provenance.date.instant, dataset2.project.dateCreated.value).generateOne
            )
            .fold(errors => fail(errors.intercalate("; ")), identity)

          loadToStore(dataset1, dataset2, dataset2Deleted)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results should contain theSameElementsAs List(dataset1.toDatasetSearchResult(projectsCount = 1))
        }

      s"not return deleted datasets when the given phrase is $maybePhrase" +
        "- case with forks on renku created datasets and the fork dataset is deleted" in new TestCase {

          val dataset     = datasetEntities(datasetProvenanceInternal).generateOne
          val datasetFork = dataset.forkProject().fork
          val datasetForkDeleted = datasetFork
            .invalidate(invalidationTimes(datasetFork.provenance.date).generateOne)
            .fold(errors => fail(errors.intercalate("; ")), identity)

          loadToStore(dataset, datasetFork, datasetForkDeleted)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results should contain theSameElementsAs List(dataset.toDatasetSearchResult(projectsCount = 1))

        }

      s"not return deleted datasets when the given phrase is $maybePhrase" +
        "- case with forks on renku created datasets and original dataset is deleted" in new TestCase {

          val dataset = datasetEntities(datasetProvenanceInternal).generateOne

          val datasetFork = dataset.forkProject().fork
          val datasetDeleted = dataset
            .invalidate(invalidationTimes(dataset.provenance.date).generateOne)
            .fold(errors => fail(errors.intercalate("; ")), identity)

          loadToStore(dataset, datasetFork, datasetDeleted)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results should contain theSameElementsAs List(datasetFork.toDatasetSearchResult(projectsCount = 1))

        }

      s"not return deleted datasets when the given phrase is $maybePhrase" +
        "- case with modification on renku created datasets" in new TestCase {

          val dataset         = datasetEntities(datasetProvenanceInternal).generateOne
          val datasetModified = modifiedDatasetEntities(dataset).generateOne
          val datasetModifiedDeleted = datasetModified
            .invalidate(invalidationTimes(datasetModified.provenance.date).generateOne)
            .fold(errors => fail(errors.intercalate("; ")), identity)

          loadToStore(dataset, datasetModified, datasetModifiedDeleted)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
            .unsafeRunSync()

          result.results should contain theSameElementsAs Nil

        }
    }
  }

  "findDatasets - some phrase given" should {

    "returns all datasets containing the phrase - " +
      "case with no shared SameAs and no modifications" in new TestCase {

        val phrase               = phrases.generateOne.value
        val dataset1             = datasetEntities(datasetProvenanceInternal).generateOne.makeNameContaining(phrase)
        val dataset2             = datasetEntities(datasetProvenanceInternal).generateOne.makeDescContaining(phrase)
        val dataset3             = datasetEntities(datasetProvenanceInternal).generateOne.makeCreatorNameContaining(phrase)
        val dataset4             = datasetEntities(datasetProvenanceInternal).generateOne.makeTitleContaining(phrase)
        val dataset5             = datasetEntities(datasetProvenanceInternal).generateOne.makeKeywordsContaining(phrase)
        val datasetWithoutPhrase = datasetEntities(ofAnyProvenance).generateOne

        loadToStore(dataset1, dataset2, dataset3, dataset4, dataset5, datasetWithoutPhrase)

        val result = datasetsFinder
          .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
          .unsafeRunSync()

        result.results shouldBe List(dataset1, dataset2, dataset3, dataset4, dataset5)
          .map(_.toDatasetSearchResult(projectsCount = 1))
          .sortBy(_.title)

        result.pagingInfo.total shouldBe Total(5)
      }

    "return no results if there is no matching dataset" in new TestCase {

      val dataset = datasetEntities(datasetProvenanceInternal).generateOne

      loadToStore(dataset)

      val result = datasetsFinder
        .findDatasets(Some(phrases.generateOne), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
        .unsafeRunSync()

      result.results.isEmpty  shouldBe true
      result.pagingInfo.total shouldBe Total(0)
    }

    "return no datasets if the match was only in an older version which is not used anymore" in new TestCase {

      val phrase       = phrases.generateOne
      val original     = datasetEntities(datasetProvenanceInternal).generateOne.makeKeywordsContaining(phrase.value)
      val modification = modifiedDatasetEntities(original).generateOne

      loadToStore(original, modification)

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
        .unsafeRunSync()
        .results
        .isEmpty shouldBe true
    }

    "return datasets matching the criteria excluding datasets which were modified and does not match anymore" in new TestCase {

      val phrase = phrases.generateOne
      val dataset1 :: dataset2 :: Nil =
        importedExternalDatasetEntities(2).generateOne.map(_.makeKeywordsContaining(phrase.value))
      val dataset2Modification = modifiedDatasetEntities(dataset2).generateOne

      loadToStore(dataset1, dataset2, dataset2Modification)

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
        .unsafeRunSync()
        .results shouldBe List(dataset1.toDatasetSearchResult(projectsCount = 1))
    }

    "return datasets matching the criteria after modification" in new TestCase {

      val phrase                      = phrases.generateOne
      val dataset1 :: dataset2 :: Nil = importedExternalDatasetEntities(2).generateOne
      val dataset2Modification        = modifiedDatasetEntities(dataset2).generateOne.makeKeywordsContaining(phrase.value)

      loadToStore(dataset1, dataset2, dataset2Modification)

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
        .unsafeRunSync()
        .results shouldBe List(dataset2Modification.toDatasetSearchResult(projectsCount = 1))
    }

    "return no datasets if the criteria is matched somewhere in the middle of the modification hierarchy" in new TestCase {

      val phrase  = phrases.generateOne
      val dataset = datasetEntities(datasetProvenanceInternal).generateOne
      val datasetModification1 = modifiedDatasetEntities(dataset).generateOne
        .makeKeywordsContaining(phrase.value)
      val datasetModification2 = modifiedDatasetEntities(datasetModification1).generateOne

      loadToStore(dataset, datasetModification1, datasetModification2)

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
        .unsafeRunSync()
        .results
        .isEmpty shouldBe true
    }

    "return datasets matching the criteria excluding datasets which were modified on forks and does not match anymore" in new TestCase {

      val phrase = phrases.generateOne
      val dataset1 = datasetEntities(datasetProvenanceInternal).generateOne
        .makeKeywordsContaining(phrase.value)
      val dataset1Fork             = dataset1.forkProject().fork
      val dataset1ForkModification = modifiedDatasetEntities(dataset1Fork).generateOne

      loadToStore(dataset1, dataset1Fork, dataset1ForkModification)

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
        .unsafeRunSync()
        .results shouldBe List(dataset1.toDatasetSearchResult(projectsCount = 1))
    }

    "return datasets matching the criteria after modification of the fork" in new TestCase {

      val phrase       = phrases.generateOne
      val dataset1     = datasetEntities(datasetProvenanceInternal).generateOne
      val dataset1Fork = dataset1.forkProject().fork
      val dataset1ForkModified = modifiedDatasetEntities(dataset1Fork).generateOne
        .makeKeywordsContaining(phrase.value)

      loadToStore(dataset1, dataset1Fork, dataset1ForkModified)

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
        .unsafeRunSync()
        .results shouldBe List(dataset1ForkModified.toDatasetSearchResult(projectsCount = 1))
    }

    s"not return deleted datasets even if the phrase match" +
      "- case with unrelated datasets" in new TestCase {
        val phrase = phrases.generateOne

        val dataset = datasetEntities(datasetProvenanceInternal).generateOne.makeDescContaining(phrase.value)
        val datasetDeleted = dataset
          .invalidate(invalidationTimes(dataset.provenance.date).generateOne)
          .fold(errors => fail(errors.intercalate("; ")), identity)

        loadToStore(dataset, datasetDeleted)

        datasetsFinder
          .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
          .unsafeRunSync()
          .results shouldBe Nil

      }
  }

  "findDatasets with explicit sorting given" should {

    s"return datasets with name, description or creator matching the given phrase sorted by $TitleProperty" in new TestCase {
      forAll(
        datasetEntities(datasetProvenanceInternal),
        datasetEntities(datasetProvenanceInternal),
        datasetEntities(datasetProvenanceInternal),
        datasetEntities(datasetProvenanceInternal)
      ) { (dataset1Orig, dataset2Orig, dataset3Orig, nonPhrased) =>
        val phrase                         = phrases.generateOne
        val (dataset1, dataset2, dataset3) = addPhraseToVariousFields(phrase, dataset1Orig, dataset2Orig, dataset3Orig)

        loadToStore(dataset1, dataset2, dataset3, nonPhrased)

        val results = datasetsFinder
          .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
          .unsafeRunSync()
          .results

        results shouldBe List(dataset1, dataset2, dataset3)
          .map(_.toDatasetSearchResult(1))
          .sortBy(_.title)
      }
    }

    s"return datasets with name, description or creator matching the given phrase sorted by $DatePublishedProperty" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) = addPhraseToVariousFields(
        phrase,
        datasetEntities(datasetProvenanceImportedExternal).generateOne,
        datasetEntities(datasetProvenanceImportedExternal).generateOne,
        datasetEntities(datasetProvenanceImportedExternal).generateOne
      )

      loadToStore(dataset1, dataset2, dataset3, datasetEntities(datasetProvenanceInternal).generateOne)

      val results = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(DatePublishedProperty, Direction.Desc), PagingRequest.default, None)
        .unsafeRunSync()
        .results

      results shouldBe List(dataset1, dataset2, dataset3)
        .map(_.toDatasetSearchResult(projectsCount = 1))
        .sortBy(_.date.instant)
        .reverse
    }

    s"return datasets with name, description or creator matching the given phrase sorted by $DateProperty" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) = addPhraseToVariousFields(
        phrase,
        datasetEntities(datasetProvenanceInternal).generateOne,
        datasetEntities(datasetProvenanceInternal).generateOne,
        datasetEntities(datasetProvenanceInternal).generateOne
      )

      loadToStore(dataset1, dataset2, dataset3, datasetEntities(datasetProvenanceInternal).generateOne)

      val results = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(DateProperty, Direction.Desc), PagingRequest.default, None)
        .unsafeRunSync()
        .results

      results shouldBe List(dataset1, dataset2, dataset3)
        .map(_.toDatasetSearchResult(projectsCount = 1))
        .sortBy(_.date.instant)
        .reverse
    }

    s"return datasets with name, description or creator matching the given phrase sorted by $ProjectsCountProperty" in new TestCase {
      val phrase = phrases.generateOne

      val (dataset1, dataset2, dataset3) = addPhraseToVariousFields(
        phrase,
        datasetEntities(datasetProvenanceInternal).generateOne,
        datasetEntities(datasetProvenanceInternal).generateOne,
        datasetEntities(datasetProvenanceInternal).generateOne
      )

      val datasets = List(
        List(dataset1, dataset1.forkProject().fork),
        List(dataset2, dataset2.forkProject().fork, dataset2.forkProject().fork),
        List(dataset3),
        List(datasetEntities(datasetProvenanceInternal).generateOne)
      )

      loadToStore(datasets.flatten: _*)

      val results = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(ProjectsCountProperty, Direction.Asc), PagingRequest.default, None)
        .unsafeRunSync()
        .results

      results shouldBe datasets
        .flatMap(_.toDatasetSearchResult(matchIdFrom = results))
        .sortBy(_.projectsCount)
    }
  }

  "findDatasets with explicit paging request" should {

    "return the requested page of datasets matching the given phrase" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) = addPhraseToVariousFields(
        phrase,
        datasetEntities(datasetProvenanceInternal).generateOne,
        datasetEntities(datasetProvenanceInternal).generateOne,
        datasetEntities(datasetProvenanceInternal).generateOne
      )

      loadToStore(dataset1, dataset2, dataset3, datasetEntities(datasetProvenanceInternal).generateOne)

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), pagingRequest, None)
        .unsafeRunSync()

      result.results shouldBe List(dataset1, dataset2, dataset3)
        .map(_.toDatasetSearchResult(projectsCount = 1))
        .sortBy(_.title)
        .get(1)
        .toList

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    "return no results if the requested page does not exist" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) = addPhraseToVariousFields(
        phrase,
        datasetEntities(datasetProvenanceInternal).generateOne,
        datasetEntities(datasetProvenanceInternal).generateOne,
        datasetEntities(datasetProvenanceInternal).generateOne
      )

      loadToStore(dataset1, dataset2, dataset3)

      val pagingRequest = PagingRequest(Page(2), PerPage(3))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), pagingRequest, None)
        .unsafeRunSync()

      result.results                  shouldBe Nil
      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }
  }

  "findDatasets with unauthorized user" should {
    List(Visibility.Private, Visibility.Internal).foreach { nonPublic =>
      s"not return dataset from project with visibility $nonPublic" in new TestCase {
        val publicDataset = datasetEntities(
          provenanceGen = datasetProvenanceInternal,
          projectsGen = projectEntities[ForksCount.Zero](visibilityPublic)
        ).generateOne

        val privateDataset = datasetEntities(
          provenanceGen = datasetProvenanceInternal,
          projectsGen = projectEntities[ForksCount.Zero](fixed(nonPublic))
        ).generateOne

        loadToStore(publicDataset, privateDataset)

        val result = datasetsFinder
          .findDatasets(None, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
          .unsafeRunSync()

        result.results shouldBe List(publicDataset.toDatasetSearchResult(projectsCount = 1))

        result.pagingInfo.total shouldBe Total(1)
      }

      s"not count projects with visibility $nonPublic" in new TestCase {
        val publicDataset = datasetEntities(
          provenanceGen = datasetProvenanceInternal,
          projectsGen = projectEntities[ForksCount.Zero](visibilityPublic)
        ).generateOne

        val publicDatasetOnPrivateProject = publicDataset
          .importTo(projectEntities[ForksCount.Zero](visibilityNonPublic).generateOne)

        loadToStore(publicDataset, publicDatasetOnPrivateProject)

        val result = datasetsFinder
          .findDatasets(None, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default, None)
          .unsafeRunSync()

        result.results          shouldBe List(publicDataset.toDatasetSearchResult(projectsCount = 1))
        result.pagingInfo.total shouldBe Total(1)
      }
    }
  }

  "findDatasets with authorized user" should {
    s"return public datasets and private datasets from project the user is a member of" in new TestCase {
      val userWithGitlabId = personEntities(userGitLabIds.toGeneratorOfSomes).generateOne
      val publicDataset = datasetEntities(
        provenanceGen = datasetProvenanceInternal,
        projectsGen = projectEntities[ForksCount.Zero](visibilityPublic)
      ).generateOne

      val privateDatasetWithAccess = datasetEntities(
        provenanceGen = datasetProvenanceInternal,
        projectsGen = projectEntities[ForksCount.Zero](visibilityNonPublic).map(_.copy(members = Set(userWithGitlabId)))
      ).generateOne

      val privateDatasetWithoutAccess = datasetEntities(
        provenanceGen = datasetProvenanceInternal,
        projectsGen = projectEntities[ForksCount.Zero](visibilityNonPublic)
      ).generateOne

      loadToStore(publicDataset, privateDatasetWithAccess, privateDatasetWithoutAccess)

      val result = datasetsFinder
        .findDatasets(None,
                      Sort.By(TitleProperty, Direction.Asc),
                      PagingRequest.default,
                      userWithGitlabId.toAuthUser.some
        )
        .unsafeRunSync()

      val datasetsList = List(
        publicDataset.toDatasetSearchResult(projectsCount = 1),
        privateDatasetWithAccess.toDatasetSearchResult(projectsCount = 1)
      ).sortBy(_.title)

      result.results shouldBe datasetsList

      result.pagingInfo.total shouldBe Total(2)
    }

    s"return public datasets and private datasets from project the user is a member of " +
      s"but not count project the user is not a member of" in new TestCase {
        val userWithGitlabId = personEntities(userGitLabIds.toGeneratorOfSomes).generateOne
        val publicDataset = datasetEntities(
          provenanceGen = datasetProvenanceInternal,
          projectsGen = projectEntities[ForksCount.Zero](visibilityPublic)
        ).generateOne

        val privateDataset = datasetEntities(
          provenanceGen = datasetProvenanceInternal,
          projectsGen = projectEntities[ForksCount.Zero](visibilityNonPublic)
        ).generateOne

        val privateDatasetWithAccess = privateDataset.importTo(
          projectEntities[ForksCount.Zero](visibilityNonPublic).generateOne
            .copy(members = Set(userWithGitlabId))
        )

        loadToStore(privateDataset, publicDataset, privateDatasetWithAccess)

        val result = datasetsFinder
          .findDatasets(None,
                        Sort.By(TitleProperty, Direction.Asc),
                        PagingRequest.default,
                        userWithGitlabId.toAuthUser.some
          )
          .unsafeRunSync()

        val datasetsList = List(
          publicDataset.toDatasetSearchResult(projectsCount = 1),
          privateDatasetWithAccess.toDatasetSearchResult(projectsCount = 1)
        ).sortBy(_.title)

        result.results          shouldBe datasetsList
        result.pagingInfo.total shouldBe Total(2)
      }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val datasetsFinder = new DatasetsFinderImpl(
      rdfStoreConfig,
      new CreatorsFinder(rdfStoreConfig, logger, timeRecorder),
      logger,
      timeRecorder
    )
  }

  implicit class PersonOps(person: Person) {
    lazy val toAuthUser: AuthUser = AuthUser(person.maybeGitLabId.get, accessTokens.generateOne)
  }

  def addPhraseToVariousFields[P <: Dataset.Provenance](
      containingPhrase: Phrase,
      dataset1Orig:     Dataset[P],
      dataset2Orig:     Dataset[P],
      dataset3Orig:     Dataset[P]
  ): (Dataset[P], Dataset[P], Dataset[P]) = {
    val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(containingPhrase.toString)
    val dataset1 = dataset1Orig.makeTitleContaining(nonEmptyPhrase.value)
    val dataset2 = dataset2Orig.makeDescContaining(nonEmptyPhrase.value)
    val dataset3 = dataset3Orig.makeKeywordsContaining(nonEmptyPhrase.value)
    (dataset1, dataset2, dataset3)
  }

  private implicit class DatasetOps(dataset: Dataset[_ <: Dataset.Provenance]) {

    def toDatasetSearchResult(projectsCount: Int)(implicit renkuBaseUrl: RenkuBaseUrl): DatasetSearchResult =
      DatasetSearchResult(
        dataset.identification.identifier,
        dataset.identification.title,
        dataset.identification.name,
        dataset.additionalInfo.maybeDescription,
        dataset.provenance.creators.map(_.to[DatasetCreator]),
        dataset.provenance.date,
        dataset.project.path,
        ProjectsCount(projectsCount),
        dataset.additionalInfo.keywords.sorted,
        dataset.additionalInfo.images
      )
  }

  private implicit class DatasetsListOps(datasets: List[Dataset[_ <: Dataset.Provenance]]) {

    def toDatasetSearchResult(matchIdFrom: List[DatasetSearchResult]): Option[DatasetSearchResult] =
      datasets
        .find(dataset => matchIdFrom.exists(_.id == dataset.identifier))
        .map(_.toDatasetSearchResult(projectsCount = datasets.size))
  }
}
