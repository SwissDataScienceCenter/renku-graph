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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.{clientExceptions, connectivityExceptions, sparqlQueries, unexpectedResponseExceptions}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.datasets.{InternalSameAs, ResourceId, TopmostSameAs}
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.http.client.RestClientError
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.rdfstore.SparqlQuery
import io.renku.triplesgenerator.events.categories.triplesgenerated.ProjectFunctions
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.Queries
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.TransformationStepsCreator.TransformationRecoverableError
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class DatasetTransformerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "createTransformationStep" should {

    "find all internally imported datasets, " +
      "find the topmostSameAs for their sameAs and resourceId, " +
      "update the datasets with the topmostSameAs found for sameAs in KG " +
      "and create update queries" in new TestCase {

        val (dataset1 ::~ dataset2, project) = anyProjectEntities
          .addDataset(datasetEntities(provenanceImportedInternal))
          .addDataset(datasetEntities(provenanceImportedInternal))
          .generateOne
          .bimap(
            _.bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]],
                    _.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
            ),
            _.to[entities.Project]
          )

        val parentTopmostSameAs = datasetTopmostSameAs.generateOne
        findingParentTopmostSameAsFor(dataset1.provenance.sameAs, returning = parentTopmostSameAs.some.pure[Try])
        findingTopmostSameAsFor(dataset1.identification.resourceId, returning = Option.empty[TopmostSameAs].pure[Try])
        val dataset1Queries = sparqlQueries.generateList()
        prepareQueriesForSameAs(dataset1 -> None, returning = dataset1Queries)
        val updatedDataset1        = dataset1.update(parentTopmostSameAs)
        val dataset1CleanUpQueries = sparqlQueries.generateList()
        prepareQueriesForTopmostSameAsCleanup(updatedDataset1 -> parentTopmostSameAs.some,
                                              returning = dataset1CleanUpQueries
        )

        findingParentTopmostSameAsFor(dataset2.provenance.sameAs, returning = None.pure[Try])
        val dataset2KgTopmostSameAs = datasetTopmostSameAs.generateSome
        findingTopmostSameAsFor(dataset2.identification.resourceId, returning = dataset2KgTopmostSameAs.pure[Try])
        val dataset2Queries = sparqlQueries.generateList()
        prepareQueriesForSameAs(dataset2 -> dataset2KgTopmostSameAs, returning = dataset2Queries)
        val dataset2CleanUpQueries = sparqlQueries.generateList()
        prepareQueriesForTopmostSameAsCleanup(dataset2 -> None, returning = dataset2CleanUpQueries)

        val step = transformer.createTransformationStep
        step.name.value shouldBe "Dataset Details Updates"

        val Success(Right((updatedProject, queries))) = (step run project).value
        updatedProject shouldBe ProjectFunctions.update(dataset1, updatedDataset1)(project)
        queries shouldBe Queries.postDataQueriesOnly(
          dataset1Queries ::: dataset1CleanUpQueries ::: dataset2Queries ::: dataset2CleanUpQueries
        )
      }

    "prepare updates for deleted datasets" in new TestCase {
      val (internal ::~ _ ::~ importedExternal ::~ _ ::~ ancestorInternal ::~ _ ::~ ancestorExternal ::~ _,
           projectWithAllDatasets
      ) = anyProjectEntities
        .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
        .addDatasetAndInvalidation(datasetEntities(provenanceImportedExternal))
        .addDatasetAndInvalidation(datasetEntities(provenanceImportedInternalAncestorInternal))
        .addDatasetAndInvalidation(datasetEntities(provenanceImportedInternalAncestorExternal))
        .generateOne
      val (beforeModification, modification, modificationInvalidated) =
        datasetAndModificationEntities(provenanceInternal,
                                       projectDateCreated = projectWithAllDatasets.dateCreated
        ).map { case internal ::~ modified => (internal, modified, modified.invalidateNow) }.generateOne

      val entitiesProjectWithAllDatasets = projectWithAllDatasets
        .addDatasets(beforeModification, modification, modificationInvalidated)
        .to[entities.Project]

      val internalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: entities.Dataset[entities.Dataset.Provenance.Internal])(
          _: entities.Dataset.Provenance.Internal.type
        ))
        .expects(internal.to[entities.Dataset[entities.Dataset.Provenance.Internal]], *)
        .returning(internalDsQueries)

      val importedExternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: entities.Dataset[entities.Dataset.Provenance.ImportedExternal])(
          _: entities.Dataset.Provenance.ImportedExternal.type
        ))
        .expects(importedExternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]], *)
        .returning(importedExternalDsQueries)

      val ancestorInternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: entities.Dataset[entities.Dataset.Provenance.ImportedInternal])(
          _: entities.Dataset.Provenance.ImportedInternal.type
        ))
        .expects(ancestorInternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]], *)
        .returning(ancestorInternalDsQueries)

      val ancestorExternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdatesWhenInvalidated(_: entities.Dataset[entities.Dataset.Provenance.ImportedInternal])(
          _: entities.Dataset.Provenance.ImportedInternal.type
        ))
        .expects(ancestorExternal.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]], *)
        .returning(ancestorExternalDsQueries)

      val step = transformer.createTransformationStep

      val Success(Right((updatedProject, queries))) = step.run(entitiesProjectWithAllDatasets).value

      updatedProject shouldBe entitiesProjectWithAllDatasets
      queries.postDataUploadQueries should contain theSameElementsAs (internalDsQueries ::: importedExternalDsQueries ::: ancestorInternalDsQueries ::: ancestorExternalDsQueries)
      queries.preDataUploadQueries shouldBe List.empty[SparqlQuery]
    }

    "return the ProcessingRecoverableFailure if calls to KG fails with a network or HTTP error" in new TestCase {
      val (dataset, project) = anyProjectEntities
        .addDataset(datasetEntities(provenanceImportedInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]], _.to[entities.Project])

      val exception = recoverableClientErrors.generateOne
      findingParentTopmostSameAsFor(dataset.provenance.sameAs,
                                    returning = exception.raiseError[Try, Option[TopmostSameAs]]
      )

      val step = transformer.createTransformationStep

      val Success(Left(recoverableError)) = step.run(project).value

      recoverableError            shouldBe a[TransformationRecoverableError]
      recoverableError.getMessage shouldBe "Problem finding dataset details in KG"
    }

    "fail with NonRecoverableFailure if finding calls to KG fails with an unknown exception" in new TestCase {
      val (dataset, project) = anyProjectEntities
        .addDataset(datasetEntities(provenanceImportedInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]], _.to[entities.Project])

      val exception = exceptions.generateOne
      findingParentTopmostSameAsFor(dataset.provenance.sameAs,
                                    returning = exception.raiseError[Try, Option[TopmostSameAs]]
      )

      val step = transformer.createTransformationStep

      val Failure(nonRecoverableError) = step.run(project).value
      nonRecoverableError shouldBe exception
    }
  }

  private trait TestCase {
    val kgDatasetInfoFinder = mock[KGDatasetInfoFinder[Try]]
    val updatesCreator      = mock[UpdatesCreator]
    val transformer         = new DatasetTransformerImpl[Try](kgDatasetInfoFinder, updatesCreator, ProjectFunctions)

    def findingParentTopmostSameAsFor(sameAs: InternalSameAs, returning: Try[Option[TopmostSameAs]]) =
      (kgDatasetInfoFinder
        .findParentTopmostSameAs(_: InternalSameAs)(_: InternalSameAs.type))
        .expects(sameAs, *)
        .returning(returning)

    def findingTopmostSameAsFor(resourceId: ResourceId, returning: Try[Option[TopmostSameAs]])(implicit
        ev:                                 TopmostSameAs.type
    ) = (kgDatasetInfoFinder
      .findTopmostSameAs(_: ResourceId)(_: ResourceId.type))
      .expects(resourceId, *)
      .returning(returning)

    def prepareQueriesForSameAs(
        dsAndMaybeTopmostSameAs: (entities.Dataset[entities.Dataset.Provenance.ImportedInternal],
                                  Option[TopmostSameAs]
        ),
        returning: List[SparqlQuery]
    ) = (updatesCreator
      .prepareUpdates(_: entities.Dataset[entities.Dataset.Provenance.ImportedInternal], _: Option[TopmostSameAs]))
      .expects(dsAndMaybeTopmostSameAs._1, dsAndMaybeTopmostSameAs._2)
      .returning(returning)

    def prepareQueriesForTopmostSameAsCleanup(
        dsAndMaybeTopmostSameAs: (entities.Dataset[entities.Dataset.Provenance.ImportedInternal],
                                  Option[TopmostSameAs]
        ),
        returning: List[SparqlQuery]
    ) = (updatesCreator
      .prepareTopmostSameAsCleanup(_: entities.Dataset[entities.Dataset.Provenance.ImportedInternal],
                                   _: Option[TopmostSameAs]
      ))
      .expects(dsAndMaybeTopmostSameAs._1, dsAndMaybeTopmostSameAs._2)
      .returning(returning)
  }

  private lazy val recoverableClientErrors: Gen[RestClientError] =
    Gen.oneOf(clientExceptions, connectivityExceptions, unexpectedResponseExceptions, Gen.const(UnauthorizedException))
}
