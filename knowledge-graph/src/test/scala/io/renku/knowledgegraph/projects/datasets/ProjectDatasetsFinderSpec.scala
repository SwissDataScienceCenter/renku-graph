/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.datasets

import cats.effect.IO
import cats.syntax.all._
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.datasets
import io.renku.graph.model.testentities._
import io.renku.http.rest.Sorting
import io.renku.knowledgegraph.projects.datasets.Endpoint.Criteria
import Criteria.Sort._
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder, TSClient}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectDatasetsFinderSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks {

  it should "return the very last modification of a dataset for the given project" in {

    val (original -> modification1, project) =
      renkuProjectEntities(anyVisibility).addDatasetAndModification(datasetEntities(provenanceInternal)).generateOne
    val (modification2, projectComplete) = project.addDataset(modification1.createModification())
    val otherProject =
      renkuProjectEntities(anyVisibility).withDatasets(datasetEntities(provenanceNonModified)).generateOne

    provisionTestProjects(otherProject, projectComplete) >>
      datasetsFinder.findProjectDatasets(Criteria(projectComplete.slug)).asserting {
        _.results shouldBe List(
          ProjectDataset(
            modification2.identification.identifier,
            datasets.OriginalIdentifier(original.identification.identifier),
            modification2.identification.name,
            modification2.identification.slug,
            original.provenance.date,
            datasets.DateModified(modification2.provenance.date).some,
            modification2.provenance.derivedFrom.asRight,
            modification2.additionalInfo.images
          )
        )
      }
  }

  it should "return non-modified datasets and the very last modifications of project's datasets" in {

    val (dataset1 -> dataset2 -> modified2, project) = renkuProjectEntities(anyVisibility)
      .addDataset(datasetEntities(provenanceImportedExternal))
      .addDatasetAndModification(datasetEntities(provenanceInternal))
      .generateOne

    provisionTestProject(project) >>
      datasetsFinder.findProjectDatasets(Criteria(project.slug)).asserting {
        _.results should contain theSameElementsAs List(
          ProjectDataset(
            dataset1.identification.identifier,
            datasets.OriginalIdentifier(dataset1.identification.identifier),
            dataset1.identification.name,
            dataset1.identification.slug,
            dataset1.provenance.date,
            maybeDateModified = None,
            dataset1.provenance.sameAs.asLeft,
            dataset1.additionalInfo.images
          ),
          ProjectDataset(
            modified2.identification.identifier,
            datasets.OriginalIdentifier(dataset2.identification.identifier),
            modified2.identification.name,
            modified2.identification.slug,
            dataset2.provenance.date,
            datasets.DateModified(modified2.provenance.date).some,
            modified2.provenance.derivedFrom.asRight,
            modified2.additionalInfo.images
          )
        )
      }
  }

  it should "return all datasets of the given project without merging datasets having the same sameAs" in {

    val (original, originalProject) =
      anyRenkuProjectEntities.addDataset(datasetEntities(provenanceInternal)).generateOne
    val (dataset1 -> dataset2, project) =
      anyRenkuProjectEntities.importDataset(original).importDataset(original).generateOne

    assume(dataset1.provenance.topmostSameAs == dataset2.provenance.topmostSameAs)
    assume(dataset1.provenance.topmostSameAs == original.provenance.topmostSameAs)

    provisionTestProjects(originalProject, project) >>
      datasetsFinder.findProjectDatasets(Criteria(project.slug)).asserting {
        _.results should contain theSameElementsAs List(
          ProjectDataset(
            dataset1.identification.identifier,
            datasets.OriginalIdentifier(dataset1.identification.identifier),
            original.identification.name,
            dataset1.identification.slug,
            dataset1.provenance.date,
            maybeDateModified = None,
            dataset1.provenance.sameAs.asLeft,
            original.additionalInfo.images
          ),
          ProjectDataset(
            dataset2.identification.identifier,
            datasets.OriginalIdentifier(dataset2.identification.identifier),
            original.identification.name,
            dataset2.identification.slug,
            dataset2.provenance.date,
            maybeDateModified = None,
            dataset2.provenance.sameAs.asLeft,
            original.additionalInfo.images
          )
        )
      }
  }

  it should "return None if there are no datasets in the project" in {
    datasetsFinder.findProjectDatasets(Criteria(projectSlugs.generateOne)).asserting(_.results shouldBe List.empty)
  }

  it should "not returned deleted dataset" in {

    val (_ -> _ -> dataset2, project) = renkuProjectEntities(anyVisibility)
      .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
      .addDataset(datasetEntities(provenanceInternal))
      .generateOne

    provisionTestProject(project) >>
      datasetsFinder.findProjectDatasets(Criteria(project.slug)).asserting {
        _.results shouldBe List(
          ProjectDataset(
            dataset2.identification.identifier,
            datasets.OriginalIdentifier(dataset2.identification.identifier),
            dataset2.identification.name,
            dataset2.identification.slug,
            dataset2.provenance.date,
            maybeDateModified = None,
            datasets.SameAs(dataset2.provenance.topmostSameAs.value).asLeft,
            dataset2.additionalInfo.images
          )
        )
      }
  }

  it should "not returned deleted dataset when its latest version was deleted" in {

    val (_ -> modification, project) =
      renkuProjectEntities(anyVisibility).addDatasetAndModification(datasetEntities(provenanceInternal)).generateOne

    provisionTestProject(project.addDatasets(modification.invalidateNow(personEntities))) >>
      datasetsFinder.findProjectDatasets(Criteria(project.slug)).asserting(_.results shouldBe Nil)
  }

  it should "return the results sorted by dateModified if requested" in {

    val (ds1 -> ds1Modification -> ds2 -> ds2Modification, project) = renkuProjectEntities(anyVisibility)
      .addDatasetAndModification(datasetEntities(provenanceImportedExternal))
      .addDatasetAndModification(datasetEntities(provenanceInternal))
      .generateOne

    provisionTestProject(project) >>
      datasetsFinder
        .findProjectDatasets(Criteria(project.slug, Sorting(Criteria.Sort.By(ByDateModified, Direction.Desc))))
        .asserting {
          _.results shouldBe List(
            ProjectDataset(
              ds1Modification.identification.identifier,
              datasets.OriginalIdentifier(ds1.identification.identifier),
              ds1Modification.identification.name,
              ds1Modification.identification.slug,
              ds1.provenance.date,
              datasets.DateModified(ds1Modification.provenance.date).some,
              ds1Modification.provenance.derivedFrom.asRight,
              ds1Modification.additionalInfo.images
            ),
            ProjectDataset(
              ds2Modification.identification.identifier,
              datasets.OriginalIdentifier(ds2.identification.identifier),
              ds2Modification.identification.name,
              ds2Modification.identification.slug,
              ds2.provenance.date,
              datasets.DateModified(ds2Modification.provenance.date).some,
              ds2Modification.provenance.derivedFrom.asRight,
              ds2Modification.additionalInfo.images
            )
          ).sortBy(_.maybeDateModified).reverse
        }
  }

  it should "return the results sorted by dateModified with a fallback to dateCreated or datePublished for non-modified DS" in {

    val (dsImported -> dsInternal -> dsOrig -> dsOrigModification, project) = renkuProjectEntities(anyVisibility)
      .addDataset(datasetEntities(provenanceImportedExternal))
      .addDataset(datasetEntities(provenanceInternal))
      .addDatasetAndModification(datasetEntities(provenanceInternal))
      .generateOne

    provisionTestProject(project) >>
      datasetsFinder
        .findProjectDatasets(Criteria(project.slug, Sorting(Criteria.Sort.By(ByDateModified, Direction.Desc))))
        .asserting {
          _.results shouldBe List(
            ProjectDataset(
              dsImported.identification.identifier,
              datasets.OriginalIdentifier(dsImported.identification.identifier),
              dsImported.identification.name,
              dsImported.identification.slug,
              dsImported.provenance.date,
              maybeDateModified = None,
              dsImported.provenance.sameAs.asLeft,
              dsImported.additionalInfo.images
            ),
            ProjectDataset(
              dsInternal.identification.identifier,
              datasets.OriginalIdentifier(dsInternal.identification.identifier),
              dsInternal.identification.name,
              dsInternal.identification.slug,
              dsInternal.provenance.date,
              maybeDateModified = None,
              datasets.SameAs(dsInternal.provenance.topmostSameAs.value).asLeft,
              dsInternal.additionalInfo.images
            ),
            ProjectDataset(
              dsOrigModification.identification.identifier,
              datasets.OriginalIdentifier(dsOrig.identification.identifier),
              dsOrigModification.identification.name,
              dsOrigModification.identification.slug,
              dsOrig.provenance.date,
              datasets.DateModified(dsOrigModification.provenance.date).some,
              dsOrigModification.provenance.derivedFrom.asRight,
              dsOrigModification.additionalInfo.images
            )
          ).sortBy(ds => ds.maybeDateModified.map(_.value).getOrElse(ds.createdOrPublished.instant)).reverse
        }
  }

  it should "return the results sorted by name by default and if requested" in {

    val (ds1 -> ds2, project) = renkuProjectEntities(anyVisibility)
      .addDataset(datasetEntities(provenanceImportedExternal))
      .addDataset(datasetEntities(provenanceInternal))
      .generateOne

    val expectedResults = List(
      ProjectDataset(
        ds1.identification.identifier,
        datasets.OriginalIdentifier(ds1.identification.identifier),
        ds1.identification.name,
        ds1.identification.slug,
        ds1.provenance.date,
        maybeDateModified = None,
        ds1.provenance.sameAs.asLeft,
        ds1.additionalInfo.images
      ),
      ProjectDataset(
        ds2.identification.identifier,
        datasets.OriginalIdentifier(ds2.identification.identifier),
        ds2.identification.name,
        ds2.identification.slug,
        ds2.provenance.date,
        maybeDateModified = None,
        datasets.SameAs(ds2.provenance.topmostSameAs.value).asLeft,
        ds2.additionalInfo.images
      )
    ).sortBy(_.slug.value.toLowerCase)

    provisionTestProject(project) >>
      datasetsFinder
        .findProjectDatasets(Criteria(project.slug, Sorting(Criteria.Sort.By(ByName, Direction.Asc))))
        .asserting(_.results shouldBe expectedResults) >>
      datasetsFinder
        .findProjectDatasets(Criteria(project.slug))
        .asserting(_.results shouldBe expectedResults)
  }

  it should "return the requested page" in {

    val (dataset1 -> dataset2 -> modified2, project) = renkuProjectEntities(anyVisibility)
      .addDataset(datasetEntities(provenanceImportedExternal))
      .addDatasetAndModification(datasetEntities(provenanceInternal))
      .generateOne

    provisionTestProject(project) >>
      datasetsFinder
        .findProjectDatasets(Criteria(project.slug, paging = PagingRequest(Page(2), PerPage(1))))
        .asserting {
          _.results shouldBe List(
            ProjectDataset(
              dataset1.identification.identifier,
              datasets.OriginalIdentifier(dataset1.identification.identifier),
              dataset1.identification.name,
              dataset1.identification.slug,
              dataset1.provenance.date,
              maybeDateModified = None,
              dataset1.provenance.sameAs.asLeft,
              dataset1.additionalInfo.images
            ),
            ProjectDataset(
              modified2.identification.identifier,
              datasets.OriginalIdentifier(dataset2.identification.identifier),
              modified2.identification.name,
              modified2.identification.slug,
              dataset2.provenance.date,
              datasets.DateModified(modified2.provenance.date).some,
              modified2.provenance.derivedFrom.asRight,
              modified2.additionalInfo.images
            )
          ).sortBy(_.slug.value.toLowerCase).slice(1, 2)
        }
  }

  implicit override def ioLogger:    TestLogger[IO]              = TestLogger[IO]()
  private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private lazy val datasetsFinder = new ProjectDatasetsFinderImpl[IO](TSClient[IO](projectsDSConnectionInfo))
}
