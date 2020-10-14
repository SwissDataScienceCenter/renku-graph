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
import cats.syntax.all._
import io.renku.jsonld.syntax._
import ch.datascience.generators.{CommonGraphGenerators, Generators}
import ch.datascience.generators.CommonGraphGenerators.cliVersions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{Description, Identifier, PublishedDate, Title}
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
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.{Activity, Agent, Artifact, Entity, Location}
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.jsonld.JsonLD
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import ch.datascience.rdfstore.entities.Person.persons
import ch.datascience.rdfstore.entities.ProjectsGenerators.projects
import eu.timepit.refined.auto._

import scala.meta.internal.semanticdb.Tree.NonEmpty

class IODatasetsFinderSpec
    extends AnyWordSpec
    with InMemoryRdfStore
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "findDatasets - no phrase" should {

    Option(Phrase("*")) +: Option.empty[Phrase] +: Nil foreach { maybePhrase =>
      s"return all datasets when the given phrase is $maybePhrase " +
        "- case of datasets that has neither sameAs nor are imported to and/or from other projects" in new TestCase {
          val sameAs1DatasetsAndJsons = nonModifiedDatasets(
            projects = datasetProjects.toGeneratorOfNonEmptyList(minElements = 2)
          ).generateOne.toJsonLDsAndDatasets(noSameAs = true)()
          val sameAs2DatasetsAndJsons = nonModifiedDatasets(
            projects = datasetProjects.toGeneratorOfNonEmptyList(maxElements = 1)
          ).generateOne.toJsonLDsAndDatasets(noSameAs = true)()
          val sameAs3DatasetsAndJsons = nonModifiedDatasets(
            projects = datasetProjects.toGeneratorOfNonEmptyList(minElements = 2)
          ).generateOne.toJsonLDsAndDatasets(noSameAs = false)()

          loadToStore(
            (sameAs1DatasetsAndJsons ++ sameAs2DatasetsAndJsons ++ sameAs3DatasetsAndJsons).jsonLDs: _*
          )

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          val datasetsList =
            List(
              sameAs1DatasetsAndJsons.toDatasetSearchResult(matchIdFrom = result.results),
              sameAs2DatasetsAndJsons.toDatasetSearchResult(matchIdFrom = result.results),
              sameAs3DatasetsAndJsons.toDatasetSearchResult(matchIdFrom = result.results)
            ).flatten.sortBy(_.title.value)

          result.results shouldBe datasetsList

          result.pagingInfo.total shouldBe Total(datasetsList.size)
        }

      s"return all datasets when the given phrase is $maybePhrase " +
        "- case of non-modified datasets" in new TestCase {

          val datasetsAndJsons = nonModifiedDatasets()
            .generateNonEmptyList(maxElements = Refined.unsafeApply(PagingRequest.default.perPage.value))
            .toList
            .map(_.toJsonLDsAndDatasets(noSameAs = false)())

          loadToStore(datasetsAndJsons.flatMap(_.jsonLDs): _*)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results shouldBe datasetsAndJsons
            .flatMap(_.toDatasetSearchResult(matchIdFrom = result.results))
            .sortBy(_.title.value)

          result.pagingInfo.total shouldBe Total(datasetsAndJsons.size)
        }

      s"return all datasets when the given phrase is $maybePhrase " +
        "- case of shared sameAs" in new TestCase {

          val sharedSameAs    = datasetSameAs.generateOne
          val dataset1Project = datasetProjects.generateOne
          val datasets1 =
            nonModifiedDatasets().generateOne.copy(sameAs = sharedSameAs, projects = List(dataset1Project))
          val datasets2 = datasets1.copy(id = datasetIdentifiers.generateOne, projects = single.generateOne.toList)

          loadToStore(datasets1.toJsonLD()(), datasets2.toJsonLD()())

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results shouldBe List(datasets1, datasets2).toDatasetSearchResult(matchIdFrom = result.results).toList

          result.pagingInfo.total shouldBe Total(1)
        }

      s"return all datasets when the given phrase is $maybePhrase " +
        "- case of shared sameAs with modification on some projects" in new TestCase {

          val sharedSameAs = datasetSameAs.generateOne
          val dataset1     = nonModifiedDatasets(projects = single).generateOne.copy(sameAs = sharedSameAs)
          val dataset2     = dataset1.copy(id = datasetIdentifiers.generateOne, projects = single.generateOne.toList)
          val datasets2Modification = modifiedDatasetsOnFirstProject(dataset2).generateOne
            .copy(title = datasetTitles.generateOne)

          loadToStore(dataset1.toJsonLD()(), dataset2.toJsonLD()(), datasets2Modification.toJsonLD())

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results shouldBe List(List(dataset1), List(datasets2Modification))
            .flatMap(_.toDatasetSearchResult(matchIdFrom = result.results))
            .sortBy(_.title.value)

          result.pagingInfo.total shouldBe Total(2)
        }

      s"return all datasets when the given phrase is $maybePhrase " +
        "- case of shared sameAs and forks" in new TestCase {

          val sharedSameAs    = datasetSameAs.generateOne
          val dataset1Project = datasetProjects.generateOne
          val datasets1 =
            nonModifiedDatasets().generateOne.copy(sameAs = sharedSameAs, projects = List(dataset1Project))
          val datasets2 = datasets1.copy(
            id = datasetIdentifiers.generateOne,
            projects = single.generateOne.toList.map(_ shiftDateAfter dataset1Project)
          )
          val datasets2Fork = datasets2.copy(
            projects = single.generateOne.toList.map(_ shiftDateAfter dataset1Project)
          )

          loadToStore(datasets1.toJsonLD()(), datasets2.toJsonLD()(), datasets2Fork.toJsonLD()())

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results shouldBe List(datasets1, datasets2, datasets2Fork)
            .toDatasetSearchResult(matchIdFrom = result.results)
            .toList

          result.pagingInfo.total shouldBe Total(1)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case of one level of modification" in new TestCase {

          val originalDatasetsList = nonModifiedDatasets(projects = single)
            .generateNonEmptyList(maxElements = Refined.unsafeApply(PagingRequest.default.perPage.value))
            .toList
          val modifiedDatasetsList = originalDatasetsList.map { ds =>
            modifiedDatasetsOnFirstProject(ds, derivedFromOverride = ds.entityId.asDerivedFrom.some).generateOne
              .copy(name = datasetNames.generateOne)
          }

          loadToStore(originalDatasetsList flatMap (_.toJsonLDsAndDatasets(noSameAs = false)().jsonLDs): _*)
          loadToStore(modifiedDatasetsList map (_.toJsonLD()):                                           _*)

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results shouldBe modifiedDatasetsList
            .map(_.toDatasetSearchResult(projectsCount = 1))
            .sortBy(_.title.value)

          result.pagingInfo.total shouldBe Total(modifiedDatasetsList.size)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case more than one level of modification" in new TestCase {

          val original = nonModifiedDatasets(projects = single).generateOne
          val modification1 = modifiedDatasetsOnFirstProject(original).generateOne
            .copy(title = datasetTitles.generateOne)
          val modification2 = modifiedDatasetsOnFirstProject(modification1).generateOne
            .copy(title = datasetTitles.generateOne)

          loadToStore(
            original.toJsonLD()(),
            modification1.toJsonLD(topmostDerivedFrom = original.entityId.asTopmostDerivedFrom),
            modification2.toJsonLD(topmostDerivedFrom = original.entityId.asTopmostDerivedFrom)
          )

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results            should contain only modification2.toDatasetSearchResult(projectsCount = 1)
          result.pagingInfo.total shouldBe Total(1)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case if there are modified and non-modified datasets" in new TestCase {

          val dataset1 = nonModifiedDatasets(projects = single).generateOne
          val dataset1Modification = modifiedDatasetsOnFirstProject(dataset1).generateOne
            .copy(name = datasetNames.generateOne)
          val nonModifiedDataset = nonModifiedDatasets(projects = single).generateOne

          loadToStore(
            dataset1.toJsonLD()(),
            dataset1Modification.toJsonLD(topmostDerivedFrom = dataset1.entityId.asTopmostDerivedFrom),
            nonModifiedDataset.toJsonLD()()
          )

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results shouldBe List(List(dataset1Modification), List(nonModifiedDataset))
            .flatMap(_.toDatasetSearchResult(result.results))
            .sortBy(_.title.value)
          result.pagingInfo.total shouldBe Total(2)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case if datasets are modified on some projects but not all" in new TestCase {

          val projects @ _ +: project2 +: Nil =
            datasetProjects.generateNonEmptyList(minElements = 2, maxElements = 2).toList
          val dataset = nonModifiedDatasets().generateOne.copy(projects = projects)
          val datasetModification = modifiedDatasetsOnFirstProject(dataset).generateOne
            .copy(title = datasetTitles.generateOne)

          val jsonsAndDatasets = dataset.toJsonLDsAndDatasets(noSameAs = false)()
          loadToStore(
            jsonsAndDatasets.jsonLDs :+
              datasetModification.toJsonLD(topmostDerivedFrom = dataset.entityId.asTopmostDerivedFrom): _*
          )

          val result = datasetsFinder
            .findDatasets(None, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results shouldBe List(
            jsonsAndDatasets.dataset(havingOnly = project2).toDatasetSearchResult(projectsCount = 1),
            datasetModification.toDatasetSearchResult(projectsCount = 1)
          ).sortBy(_.title.value)

          result.pagingInfo.total shouldBe Total(2)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case with forks on renku created datasets" in new TestCase {

          val dataset     = nonModifiedDatasets(projects = single).generateOne
          val datasetFork = dataset.copy(projects = List(datasetProjects.generateOne))

          loadToStore(
            dataset.toJsonLD(noSameAs = true)(),
            datasetFork.toJsonLD(noSameAs = true)()
          )

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results should contain theSameElementsAs List(dataset, datasetFork)
            .toDatasetSearchResult(result.results)
            .toList

          result.pagingInfo.total shouldBe Total(1)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case with more than one level of modification and forks on the 1st level" in new TestCase {

          val dataset     = nonModifiedDatasets(projects = single).generateOne
          val datasetFork = dataset.copy(projects = List(datasetProjects.generateOne))
          val datasetModification = modifiedDatasetsOnFirstProject(dataset).generateOne
            .copy(title = datasetTitles.generateOne)

          loadToStore(
            dataset.toJsonLD()(),
            datasetFork.toJsonLD()(),
            datasetModification.toJsonLD(topmostDerivedFrom = datasetFork.entityId.asTopmostDerivedFrom)
          )

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results shouldBe List(
            datasetFork.toDatasetSearchResult(projectsCount = 1),
            datasetModification.toDatasetSearchResult(projectsCount = 1)
          ).sortBy(_.title.value)

          result.pagingInfo.total shouldBe Total(2)
        }

      s"return latest versions of datasets when the given phrase is $maybePhrase " +
        "- case with more than one level of modification and forks on not the 1st level" in new TestCase {

          val dataset = nonModifiedDatasets(projects = single).generateOne
          val datasetModification = modifiedDatasetsOnFirstProject(dataset).generateOne
            .copy(name = datasetNames.generateOne)
          val forkProject             = datasetProjects.generateOne
          val datasetModificationFork = datasetModification.copy(projects = List(forkProject))
          val datasetModificationOnFork = modifiedDatasetsOnFirstProject(datasetModificationFork).generateOne
            .copy(name = datasetNames.generateOne)

          loadToStore(
            dataset.toJsonLD()(),
            datasetModification.toJsonLD(topmostDerivedFrom = dataset.entityId.asTopmostDerivedFrom),
            datasetModificationFork.toJsonLD(topmostDerivedFrom = dataset.entityId.asTopmostDerivedFrom),
            datasetModificationOnFork.toJsonLD(topmostDerivedFrom =
              datasetModificationFork.entityId.asTopmostDerivedFrom
            )
          )

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results should contain theSameElementsAs List(
            datasetModification.toDatasetSearchResult(projectsCount = 1),
            datasetModificationOnFork.toDatasetSearchResult(projectsCount = 1)
          ).sortBy(_.title.value)

          result.pagingInfo.total shouldBe Total(2)
        }

      s"not return deleted datasets when the given phrase is $maybePhrase" +
        "- case with unrelated datasets" in new TestCase {
          val dataset0               = nonModifiedDatasets(projects = single).generateOne
          val datasetToBeInvalidated = nonModifiedDatasets(projects = single).generateOne
          loadToStore(
            dataset0.toJsonLD()(),
            datasetToBeInvalidated.toJsonLD()()
          )

          val entityWithInvalidation: Entity with Artifact = invalidationEntity(datasetToBeInvalidated.id).generateOne

          loadToStore(
            entityWithInvalidation.asJsonLD
          )

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results should contain theSameElementsAs List(
            dataset0.toDatasetSearchResult(projectsCount = 1)
          )

          result.pagingInfo.total shouldBe Total(1)
        }

      s"not return deleted forked datasets when the given phrase is $maybePhrase" +
        "- case with forks on renku created datasets" in new TestCase {
          val dataset     = nonModifiedDatasets(projects = single).generateOne
          val datasetFork = dataset.copy(projects = List(datasetProjects.generateOne))

          loadToStore(
            dataset.toJsonLD(noSameAs = true)(),
            datasetFork.toJsonLD(noSameAs = true)()
          )

          val entityWithInvalidation: Entity with Artifact = invalidationEntity(datasetFork.id).generateOne

          loadToStore(
            entityWithInvalidation.asJsonLD
          )
          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results should contain theSameElementsAs List(dataset.toDatasetSearchResult(projectsCount = 1))

          result.pagingInfo.total shouldBe Total(1)

        }

      s"not return deleted datasets when the given phrase is $maybePhrase" +
        "- case with modification on renku created datasets" in new TestCase {
          val dataset0 = nonModifiedDatasets(projects = single).generateOne
          val dataset1 = nonModifiedDatasets(projects = single).generateOne
          val dataset0Modification = modifiedDatasetsOnFirstProject(dataset0).generateOne
            .copy(name = datasetNames.generateOne)

          val entityWithInvalidation: Entity with Artifact = invalidationEntity(dataset0Modification.id).generateOne

          loadToStore(
            dataset0.toJsonLD()(),
            dataset1.toJsonLD()(),
            dataset0Modification.toJsonLD(topmostDerivedFrom = dataset0.entityId.asTopmostDerivedFrom),
            entityWithInvalidation.asJsonLD
          )

          val result = datasetsFinder
            .findDatasets(maybePhrase, Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()

          result.results            should contain theSameElementsAs List(dataset1.toDatasetSearchResult(projectsCount = 1))
          result.pagingInfo.total shouldBe Total(1)

        }
    }
  }

  "findDatasets - some phrase given" should {

    "returns all datasets containing the phrase - " +
      "case with no shared SameAs and no modifications" in new TestCase {

        val phrase = phrases.generateOne
        val sameAs1DatasetsAndJsons = nonModifiedDatasets(
          projects = nonEmptyList(datasetProjects, minElements = 2)
        ).generateOne.makeNameContaining(phrase).toJsonLDsAndDatasets(noSameAs = true)()
        val sameAs2DatasetsAndJsons = nonModifiedDatasets(
          projects = nonEmptyList(datasetProjects, maxElements = 1)
        ).generateOne.makeDescContaining(phrase).toJsonLDsAndDatasets(noSameAs = true)()
        val sameAs3DatasetsAndJsons = nonModifiedDatasets(
          projects = nonEmptyList(datasetProjects, maxElements = 1)
        ).generateOne.makeCreatorNameContaining(phrase).toJsonLDsAndDatasets(noSameAs = true)()
        val sameAs4DatasetsAndJsons = nonModifiedDatasets(
          projects = nonEmptyList(datasetProjects, maxElements = 1)
        ).generateOne.makeTitleContaining(phrase).toJsonLDsAndDatasets(noSameAs = true)()

        loadToStore(
          (sameAs1DatasetsAndJsons ++
            sameAs2DatasetsAndJsons ++
            sameAs3DatasetsAndJsons ++
            sameAs4DatasetsAndJsons ++
            nonModifiedDatasets().generateOne.toJsonLDsAndDatasets(noSameAs = true)()).jsonLDs: _*
        )

        val result = datasetsFinder
          .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
          .unsafeRunSync()

        result.results shouldBe List(sameAs1DatasetsAndJsons,
                                     sameAs2DatasetsAndJsons,
                                     sameAs3DatasetsAndJsons,
                                     sameAs4DatasetsAndJsons
        )
          .flatMap(_.toDatasetSearchResult(result.results))
          .sortBy(_.title.value)

        result.pagingInfo.total shouldBe Total(4)
      }

    "return no results if there is no matching dataset" in new TestCase {

      val dataset = nonModifiedDatasets().generateOne

      loadToStore(dataset.toJsonLDsAndDatasets(noSameAs = true)().jsonLDs: _*)

      val result = datasetsFinder
        .findDatasets(Some(phrases.generateOne), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()

      result.results          shouldBe empty
      result.pagingInfo.total shouldBe Total(0)
    }

    "return no datasets if the match was only in an older version which is not used anymore" in new TestCase {

      val phrase       = phrases.generateOne
      val project      = datasetProjects.generateOne
      val original     = nonModifiedDatasets().generateOne.copy(projects = List(project)).makeTitleContaining(phrase)
      val modification = modifiedDatasetsOnFirstProject(original).generateOne.copy(title = datasetTitles.generateOne)

      loadToStore(
        original.toJsonLDsAndDatasets(noSameAs = true)().jsonLDs :+ modification.toJsonLD(): _*
      )

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()
        .results shouldBe empty
    }

    "return datasets matching the criteria excluding datasets which were modified and does not match anymore" in new TestCase {

      val phrase       = phrases.generateOne
      val sharedSameAs = datasetSameAs.generateOne
      val dataset1 = nonModifiedDatasets(projects = single).generateOne
        .copy(sameAs = sharedSameAs)
        .makeTitleContaining(phrase)
      val dataset2 = nonModifiedDatasets(projects = single).generateOne
        .copy(sameAs = sharedSameAs)
        .makeTitleContaining(phrase)
      val dataset2Modification = modifiedDatasetsOnFirstProject(dataset2).generateOne
        .copy(title = datasetTitles.generateOne)

      loadToStore(
        List(dataset1, dataset2).flatMap(_.toJsonLDsAndDatasets(noSameAs = true)()).jsonLDs :+
          dataset2Modification.toJsonLD(): _*
      )

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()
        .results shouldBe List(dataset1.toDatasetSearchResult(projectsCount = 1))
    }

    "return datasets matching the criteria after modification" in new TestCase {

      val phrase       = phrases.generateOne
      val sharedSameAs = datasetSameAs.generateOne
      val dataset1 = nonModifiedDatasets(projects = single).generateOne
        .copy(sameAs = sharedSameAs)
      val dataset2 = nonModifiedDatasets(projects = single).generateOne
        .copy(sameAs = sharedSameAs)
      val dataset2Modification = modifiedDatasetsOnFirstProject(dataset2).generateOne
        .makeTitleContaining(phrase)

      loadToStore(
        List(dataset1, dataset2).flatMap(_.toJsonLDsAndDatasets(noSameAs = true)()).jsonLDs :+
          dataset2Modification.toJsonLD(): _*
      )

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()
        .results shouldBe List(dataset2Modification.toDatasetSearchResult(projectsCount = 1))
    }

    "return no datasets if the criteria is matched somewhere in the middle of the modification hierarchy" in new TestCase {

      val phrase       = phrases.generateOne
      val sharedSameAs = datasetSameAs.generateOne
      val dataset = nonModifiedDatasets(projects = single).generateOne
        .copy(sameAs = sharedSameAs)
      val datasetModification1 = modifiedDatasetsOnFirstProject(dataset).generateOne
        .makeTitleContaining(phrase)
      val datasetModification2 = modifiedDatasetsOnFirstProject(datasetModification1).generateOne
        .copy(title = datasetTitles.generateOne)

      loadToStore(
        dataset.toJsonLD()(),
        datasetModification1.toJsonLD(),
        datasetModification2.toJsonLD()
      )

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()
        .results shouldBe empty
    }

    "return datasets matching the criteria excluding datasets which were modified on forks and does not match anymore" in new TestCase {

      val phrase       = phrases.generateOne
      val sharedSameAs = datasetSameAs.generateOne
      val dataset1 = nonModifiedDatasets(projects = single).generateOne
        .copy(sameAs = sharedSameAs)
        .makeTitleContaining(phrase)
      val dataset2 = dataset1.copy(projects = single.generateOne.toList)
      val dataset2Modification = modifiedDatasetsOnFirstProject(dataset2).generateOne
        .copy(title = datasetTitles.generateOne)

      loadToStore(
        List(dataset1, dataset2).flatMap(_.toJsonLDsAndDatasets(noSameAs = true)()).jsonLDs :+
          dataset2Modification.toJsonLD(): _*
      )

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()
        .results shouldBe List(dataset1.toDatasetSearchResult(projectsCount = 1))
    }

    "return datasets matching the criteria after modification of the fork" in new TestCase {

      val phrase   = phrases.generateOne
      val dataset1 = nonModifiedDatasets(projects = single).generateOne
      val dataset2 = dataset1.copy(projects = single.generateOne.toList)
      val dataset2Modification = modifiedDatasetsOnFirstProject(dataset2).generateOne
        .makeTitleContaining(phrase)

      loadToStore(
        List(dataset1, dataset2).flatMap(_.toJsonLDsAndDatasets(noSameAs = true)()).jsonLDs :+
          dataset2Modification.toJsonLD(): _*
      )

      datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()
        .results shouldBe List(dataset2Modification.toDatasetSearchResult(projectsCount = 1))
    }
  }

  "findDatasets with explicit sorting given" should {

    s"return datasets with name, description or creator matching the given phrase sorted by $TitleProperty" in new TestCase {
      forAll(nonModifiedDatasets(), nonModifiedDatasets(), nonModifiedDatasets(), nonModifiedDatasets()) {
        (dataset1Orig, dataset2Orig, dataset3Orig, nonPhrased) =>
          val phrase                         = phrases.generateOne
          val (dataset1, dataset2, dataset3) = addPhrase(phrase, dataset1Orig, dataset2Orig, dataset3Orig)

          val datasetsAndJsons = List(dataset1, dataset2, dataset3, nonPhrased)
            .map(_.toJsonLDsAndDatasets(noSameAs = false)())

          loadToStore(datasetsAndJsons.flatten.jsonLDs: _*)

          val results = datasetsFinder
            .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), PagingRequest.default)
            .unsafeRunSync()
            .results

          results shouldBe datasetsAndJsons
            .flatMap(_.toDatasetSearchResult(results))
            .sortBy(_.title.value)
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

      val datasetsAndJsons = List(dataset1, dataset2, dataset3, nonModifiedDatasets().generateOne)
        .map(_.toJsonLDsAndDatasets(noSameAs = false)())

      loadToStore(datasetsAndJsons.flatten.jsonLDs: _*)

      val results = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(DatePublishedProperty, Direction.Desc), PagingRequest.default)
        .unsafeRunSync()
        .results

      results shouldBe datasetsAndJsons
        .flatMap(_.toDatasetSearchResult(results))
        .sortBy(_.published.maybeDate)
        .reverse

      implicit lazy val maybePublishedOrdering: Ordering[Option[PublishedDate]] =
        (x: Option[PublishedDate], y: Option[PublishedDate]) =>
          x -> y match {
            case (Some(left), Some(right)) => left.value.compareTo(right.value)
            case (Some(_), None)           => 1
            case (None, Some(_))           => -1
            case _                         => 0
          }
    }

    s"return datasets with name, description or creator matching the given phrase sorted by $ProjectsCountProperty" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) = addPhrase(
        phrase,
        nonModifiedDatasets(projects = nonEmptyList(datasetProjects, minElements = 4, maxElements = 4)).generateOne,
        nonModifiedDatasets(projects = nonEmptyList(datasetProjects, maxElements = 1)).generateOne,
        nonModifiedDatasets(projects = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)).generateOne
      )

      val datasetsAndJsons = List(dataset1, dataset2, dataset3, nonModifiedDatasets().generateOne)
        .map(_.toJsonLDsAndDatasets(noSameAs = false)())

      loadToStore(datasetsAndJsons.flatten.jsonLDs: _*)

      val results = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(ProjectsCountProperty, Direction.Asc), PagingRequest.default)
        .unsafeRunSync()
        .results

      results shouldBe datasetsAndJsons
        .flatMap(_.toDatasetSearchResult(results))
        .sortBy(_.projectsCount.value)
    }
  }

  "findDatasets with explicit paging request" should {

    "return the requested page of datasets matching the given phrase" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) =
        addPhrase(phrase,
                  nonModifiedDatasets().generateOne,
                  nonModifiedDatasets().generateOne,
                  nonModifiedDatasets().generateOne
        )

      val datasetsAndJsons = List(dataset1, dataset2, dataset3, nonModifiedDatasets().generateOne)
        .map(_.toJsonLDsAndDatasets(noSameAs = false)())

      loadToStore(datasetsAndJsons.flatten.jsonLDs: _*)

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      // toDatasetSearchResult(result.results) filters out datasets which does not exist in the given results
      // so that's why there's only one item
      val expectedResults = datasetsAndJsons
        .flatMap(_.toDatasetSearchResult(result.results))
        .sortBy(_.title.value)
      result.results shouldBe expectedResults

      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
    }

    "return no results if the requested page does not exist" in new TestCase {
      val phrase = phrases.generateOne
      val (dataset1, dataset2, dataset3) =
        addPhrase(phrase,
                  nonModifiedDatasets().generateOne,
                  nonModifiedDatasets().generateOne,
                  nonModifiedDatasets().generateOne
        )

      val datasetsAndJsons = List(dataset1, dataset2, dataset3, nonModifiedDatasets().generateOne)
        .map(_.toJsonLDsAndDatasets(noSameAs = false)())

      loadToStore(datasetsAndJsons.flatten.jsonLDs: _*)

      val pagingRequest = PagingRequest(Page(2), PerPage(3))

      val result = datasetsFinder
        .findDatasets(Some(phrase), Sort.By(TitleProperty, Direction.Asc), pagingRequest)
        .unsafeRunSync()

      result.results                  shouldBe Nil
      result.pagingInfo.pagingRequest shouldBe pagingRequest
      result.pagingInfo.total         shouldBe Total(3)
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
      containingPhrase: Phrase,
      dataset1Orig:     NonModifiedDataset,
      dataset2Orig:     NonModifiedDataset,
      dataset3Orig:     NonModifiedDataset
  ): (NonModifiedDataset, NonModifiedDataset, NonModifiedDataset) = {
    val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(containingPhrase.toString)
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

    def toDatasetSearchResult(projectsCount: Int): DatasetSearchResult = DatasetSearchResult(
      dataset.id,
      dataset.title,
      dataset.name,
      dataset.maybeDescription,
      dataset.published,
      ProjectsCount(projectsCount)
    )
  }

  private implicit class DatasetsListOps(datasets: List[Dataset]) {

    def toDatasetSearchResult(matchIdFrom: List[DatasetSearchResult]): Option[DatasetSearchResult] =
      datasets
        .find { dataset =>
          matchIdFrom.exists(_.id == dataset.id)
        }
        .map { dataset =>
          dataset.toDatasetSearchResult(projectsCount = datasets.size)
        }
  }

  private implicit class JsonAndProjectTuplesOps(tuples: List[(JsonLD, Dataset)]) {

    lazy val jsonLDs: List[JsonLD] = tuples.map(_._1)

    def toDatasetSearchResult(matchIdFrom: List[DatasetSearchResult]): Option[DatasetSearchResult] =
      tuples
        .find { case (_, dataset) =>
          matchIdFrom.exists(_.id == dataset.id)
        }
        .map { case (_, dataset) =>
          dataset.toDatasetSearchResult(projectsCount = tuples.size)
        }

    def dataset(havingOnly: DatasetProject): Dataset = {
      tuples
        .find { case (_, ds) =>
          ds.projects match {
            case first +: Nil => first.path == havingOnly.path
            case _            => false
          }
        } getOrElse fail(s"Cannot find dataset for project ${havingOnly.path}")
    }._2
  }

  private lazy val activities: Gen[Activity] = for {
    commitId      <- commitIds
    committedDate <- committedDates
    committer     <- persons
    project       <- projects
    cliVersion    <- cliVersions
    comment       <- nonEmptyStrings()
  } yield Activity(
    commitId,
    committedDate,
    committer,
    project,
    Agent(cliVersion),
    comment,
    None,
    None,
    None,
    Nil
  )

  private def invalidationEntity(datasetId: Identifier): Gen[Entity with Artifact] = for {
    activity <- activities
  } yield new Entity(
    activity.commitId,
    Location(".renku") / "datasets" / datasetId / "metadata.yml",
    activity.project,
    Some(activity),
    None
  ) with Artifact
}
