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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration
package datasets

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.{clientExceptions, connectivityExceptions, sparqlQueries, unexpectedResponseExceptions}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DerivedFrom, InternalSameAs, ResourceId, TopmostDerivedFrom, TopmostSameAs}
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities._
import ch.datascience.http.client.RestClientError
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.ProjectFunctions
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.TransformationRecoverableError
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class DatasetTransformerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "createTransformationStep" should {

    "find all internally imported datasets, " +
      "find the topmostSameAs for their sameAs and resourceId, " +
      "update the datasets with the topmostSameAs found for sameAs " +
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

        val kgTopmostSameAs = datasetTopmostSameAs.generateOne
        findingParentTopmostSameAsFor(dataset1.provenance.sameAs, returning = kgTopmostSameAs.some.pure[Try])
        findingTopmostSameAsFor(dataset1.identification.resourceId, returning = Option.empty[TopmostSameAs].pure[Try])
        val dataset1Queries = sparqlQueries.generateList()
        prepareQueriesForSameAs(dataset1 -> Option.empty[TopmostSameAs], returning = dataset1Queries)

        findingParentTopmostSameAsFor(dataset2.provenance.sameAs, returning = None.pure[Try])
        val dataset2KgTopmostSameAs = datasetTopmostSameAs.generateSome
        findingTopmostSameAsFor(dataset2.identification.resourceId, returning = dataset2KgTopmostSameAs.pure[Try])
        val dataset2Queries = sparqlQueries.generateList()
        prepareQueriesForSameAs(dataset2 -> dataset2KgTopmostSameAs, returning = dataset2Queries)

        val step = transformer.createTransformationStep

        step.name.value shouldBe "Dataset Details Updates"
        val Success(Right(updateResult)) = step.run(project).value
        updateResult.project shouldBe ProjectFunctions.update(dataset1, dataset1.update(kgTopmostSameAs))(project)
        updateResult.queries shouldBe (dataset1Queries ::: dataset2Queries)
      }

    "find all modified datasets, " +
      "find the topmostDerivedFrom for them, " +
      "update the datasets with the found topmostDerived if found " +
      "and update the modified datasets in the metadata" in new TestCase {

        val ((dataset1, dataset2, importedDataset, dataset3), project) = anyProjectEntities
          .addDatasetAndModification(datasetEntities(provenanceInternal))
          .addDatasetAndModification(datasetEntities(provenanceImportedExternal))
          .addDatasetAndModification(datasetEntities(provenanceImportedInternal))
          .generateOne
          .leftMap { case _ ::~ dataset1 ::~ _ ::~ dataset2 ::~ importedDataset ::~ dataset3 =>
            (
              dataset1.to[entities.Dataset[entities.Dataset.Provenance.Modified]],
              dataset2.to[entities.Dataset[entities.Dataset.Provenance.Modified]],
              importedDataset.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]],
              dataset3.to[entities.Dataset[entities.Dataset.Provenance.Modified]]
            )
          }
          .map(_.to[entities.Project])

        val kgTopmostDerivedFrom = datasetTopmostDerivedFroms.generateOne
        findingParentTopmostDerivedFromFor(dataset1.provenance.derivedFrom,
                                           returning = kgTopmostDerivedFrom.some.pure[Try]
        )
        findingTopmostDerivedFromFor(dataset1.identification.resourceId,
                                     returning = Option.empty[TopmostDerivedFrom].pure[Try]
        )
        val dataset1Queries = sparqlQueries.generateList()
        prepareQueriesForDerivedFrom(dataset1 -> Option.empty[TopmostDerivedFrom], returning = dataset1Queries)

        findingParentTopmostDerivedFromFor(dataset2.provenance.derivedFrom, returning = None.pure[Try])
        val dataset2KgTopmostDerivedFrom = datasetTopmostDerivedFroms.generateSome
        findingTopmostDerivedFromFor(dataset2.identification.resourceId,
                                     returning = dataset2KgTopmostDerivedFrom.pure[Try]
        )
        val dataset2Queries = sparqlQueries.generateList()
        prepareQueriesForDerivedFrom(dataset2 -> dataset2KgTopmostDerivedFrom, returning = dataset2Queries)

        val kgTopmostSameAs = datasetTopmostSameAs.generateSome
        findingParentTopmostSameAsFor(importedDataset.provenance.sameAs, returning = None.pure[Try])
        findingTopmostSameAsFor(importedDataset.identification.resourceId, returning = kgTopmostSameAs.pure[Try])
        val importedDatasetQueries = sparqlQueries.generateList()
        prepareQueriesForSameAs(importedDataset -> kgTopmostSameAs, returning = importedDatasetQueries)

        findingParentTopmostDerivedFromFor(dataset3.provenance.derivedFrom, returning = None.pure[Try])
        val dataset3KgTopmostDerivedFrom = datasetTopmostDerivedFroms.generateSome
        findingTopmostDerivedFromFor(dataset3.identification.resourceId,
                                     returning = dataset3KgTopmostDerivedFrom.pure[Try]
        )
        val dataset3Queries = sparqlQueries.generateList()
        prepareQueriesForDerivedFrom(dataset3 -> dataset3KgTopmostDerivedFrom, returning = dataset3Queries)

        val step = transformer.createTransformationStep

        step.name.value shouldBe "Dataset Details Updates"
        val Success(Right(updateResult)) = step.run(project).value
        updateResult.project shouldBe ProjectFunctions.update(dataset1, dataset1.update(kgTopmostDerivedFrom))(project)
        updateResult.queries   should contain theSameElementsAs (dataset1Queries ::: dataset2Queries ::: importedDatasetQueries ::: dataset3Queries)
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

      val Success(Right(updateResult)) = step.run(entitiesProjectWithAllDatasets).value

      updateResult.project shouldBe entitiesProjectWithAllDatasets
      updateResult.queries   should contain theSameElementsAs (internalDsQueries ::: importedExternalDsQueries ::: ancestorInternalDsQueries ::: ancestorExternalDsQueries)
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
      .prepareUpdates(_: entities.Dataset[entities.Dataset.Provenance.ImportedInternal], _: Option[TopmostSameAs])(
        _: TopmostSameAs.type
      ))
      .expects(dsAndMaybeTopmostSameAs._1, dsAndMaybeTopmostSameAs._2, *)
      .returning(returning)

    def findingParentTopmostDerivedFromFor(derivedFrom: DerivedFrom, returning: Try[Option[TopmostDerivedFrom]]) =
      (kgDatasetInfoFinder
        .findParentTopmostDerivedFrom(_: DerivedFrom)(_: DerivedFrom.type))
        .expects(derivedFrom, *)
        .returning(returning)

    def findingTopmostDerivedFromFor(resourceId: ResourceId, returning: Try[Option[TopmostDerivedFrom]])(implicit
        ev:                                      TopmostDerivedFrom.type
    ) = (kgDatasetInfoFinder
      .findTopmostDerivedFrom(_: ResourceId)(_: ResourceId.type))
      .expects(resourceId, *)
      .returning(returning)

    def prepareQueriesForDerivedFrom(
        dsAndMaybeTopmostDerivedFrom: (entities.Dataset[entities.Dataset.Provenance.Modified],
                                       Option[TopmostDerivedFrom]
        ),
        returning: List[SparqlQuery]
    ) = (updatesCreator
      .prepareUpdates(_: entities.Dataset[entities.Dataset.Provenance.Modified], _: Option[TopmostDerivedFrom])(
        _: TopmostDerivedFrom.type
      ))
      .expects(dsAndMaybeTopmostDerivedFrom._1, dsAndMaybeTopmostDerivedFrom._2, *)
      .returning(returning)
  }

  private lazy val recoverableClientErrors: Gen[RestClientError] =
    Gen.oneOf(clientExceptions, connectivityExceptions, unexpectedResponseExceptions, Gen.const(UnauthorizedException))
}
