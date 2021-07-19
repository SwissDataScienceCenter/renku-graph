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
import ch.datascience.graph.model.entities.Dataset
import ch.datascience.graph.model.testentities._
import ch.datascience.http.client.RestClientError
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.ProjectMetadata
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.TransformationRecoverableError
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class DatasetTransformerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "createTransformationStep" should {
    val projectMetadata = mock[ProjectMetadata]

    "find all internally imported datasets, " +
      "find the topmostSameAs for their sameAs and resourceId, " +
      "update the datasets with the topmostSameAs found for sameAs " +
      "and create update queries" in new TestCase {

        val dataset1 = datasetEntities(datasetProvenanceImportedInternal).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
        val dataset2 = datasetEntities(datasetProvenanceImportedInternal).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]

        (() => projectMetadata.findInternallyImportedDatasets).expects().returning(List(dataset1, dataset2))

        val kgTopmostSameAs = datasetTopmostSameAs.generateOne
        findingParentTopmostSameAsFor(dataset1.provenance.sameAs, returning = kgTopmostSameAs.some.pure[Try])
        findingTopmostSameAsFor(dataset1.identification.resourceId, returning = Option.empty[TopmostSameAs].pure[Try])
        val dataset1Queries = sparqlQueries.generateList()
        prepareQueriesForSameAs(dataset1 -> Option.empty[TopmostSameAs], returning = dataset1Queries)

        val updatedProjectMetadataAfterDS1Update = mock[ProjectMetadata]
        (projectMetadata
          .update(_: entities.Dataset[entities.Dataset.Provenance], _: entities.Dataset[entities.Dataset.Provenance]))
          .expects(dataset1, dataset1.update(kgTopmostSameAs))
          .returning(updatedProjectMetadataAfterDS1Update)

        findingParentTopmostSameAsFor(dataset2.provenance.sameAs, returning = None.pure[Try])
        val dataset2KgTopmostSameAs = datasetTopmostSameAs.generateSome
        findingTopmostSameAsFor(dataset2.identification.resourceId, returning = dataset2KgTopmostSameAs.pure[Try])
        val dataset2Queries = sparqlQueries.generateList()
        prepareQueriesForSameAs(dataset2 -> dataset2KgTopmostSameAs, returning = dataset2Queries)

        val updatedProjectMetadataAfterDS2Update = mock[ProjectMetadata]
        (updatedProjectMetadataAfterDS1Update
          .update(_: entities.Dataset[entities.Dataset.Provenance], _: entities.Dataset[entities.Dataset.Provenance]))
          .expects(dataset2, dataset2)
          .returning(updatedProjectMetadataAfterDS2Update)

        (() => updatedProjectMetadataAfterDS2Update.findModifiedDatasets).expects().returning(Nil)
        (() => updatedProjectMetadataAfterDS2Update.findInvalidatedDatasets).expects().returning(Nil)

        val step = transformer.createTransformationStep

        step.name.value shouldBe "Dataset Details Updates"
        val Success(Right(updateResult)) = step.run(projectMetadata).value
        updateResult.projectMetadata shouldBe updatedProjectMetadataAfterDS2Update
        updateResult.queries         shouldBe (dataset1Queries ::: dataset2Queries)
      }

    "find all modified datasets, " +
      "find the topmostDerivedFrom for them, " +
      "update the datasets with the found topmostDerived if found " +
      "and update the modified datasets in the metadata" in new TestCase {

        (() => projectMetadata.findInternallyImportedDatasets).expects().returning(Nil)

        val dataset1 = datasetEntities(datasetProvenanceModified).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.Modified]]
        val dataset2 = datasetEntities(datasetProvenanceModified).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.Modified]]

        (() => projectMetadata.findModifiedDatasets).expects().returning(List(dataset1, dataset2))

        val kgTopmostDerivedFrom = datasetTopmostDerivedFroms.generateOne
        findingParentTopmostDerivedFromFor(dataset1.provenance.derivedFrom,
                                           returning = kgTopmostDerivedFrom.some.pure[Try]
        )
        findingTopmostDerivedFromFor(dataset1.identification.resourceId,
                                     returning = Option.empty[TopmostDerivedFrom].pure[Try]
        )
        val dataset1Queries = sparqlQueries.generateList()
        prepareQueriesForDerivedFrom(dataset1 -> Option.empty[TopmostDerivedFrom], returning = dataset1Queries)

        val updatedProjectMetadataAfterDs1Update = mock[ProjectMetadata]
        (projectMetadata
          .update(_: entities.Dataset[entities.Dataset.Provenance], _: entities.Dataset[entities.Dataset.Provenance]))
          .expects(dataset1, dataset1.update(kgTopmostDerivedFrom))
          .returning(updatedProjectMetadataAfterDs1Update)

        findingParentTopmostDerivedFromFor(dataset2.provenance.derivedFrom, returning = None.pure[Try])
        val dataset2KgTopmostDerivedFrom = datasetTopmostDerivedFroms.generateSome
        findingTopmostDerivedFromFor(dataset2.identification.resourceId,
                                     returning = dataset2KgTopmostDerivedFrom.pure[Try]
        )
        val dataset2Queries = sparqlQueries.generateList()
        prepareQueriesForDerivedFrom(dataset2 -> dataset2KgTopmostDerivedFrom, returning = dataset2Queries)

        val updatedProjectMetadataAfterDs2Update = mock[ProjectMetadata]
        (updatedProjectMetadataAfterDs1Update
          .update(_: entities.Dataset[entities.Dataset.Provenance], _: entities.Dataset[entities.Dataset.Provenance]))
          .expects(dataset2, dataset2)
          .returning(updatedProjectMetadataAfterDs2Update)

        (() => updatedProjectMetadataAfterDs2Update.findInvalidatedDatasets).expects().returning(Nil)

        val step = transformer.createTransformationStep

        step.name.value shouldBe "Dataset Details Updates"
        val Success(Right(updateResult)) = step.run(projectMetadata).value
        updateResult.projectMetadata shouldBe updatedProjectMetadataAfterDs2Update
        updateResult.queries         shouldBe (dataset1Queries ::: dataset2Queries)
      }

    "prepare updates for deleted datasets" in new TestCase {
      (() => projectMetadata.findInternallyImportedDatasets).expects().returning(Nil)
      (() => projectMetadata.findModifiedDatasets).expects().returning(Nil)

      val internal = {
        val ds = datasetEntities(datasetProvenanceInternal).generateOne
          .copy(parts = Nil)
          .to[entities.Dataset[entities.Dataset.Provenance.Internal]]
        ds.copy(maybeInvalidationTime = invalidationTimes(ds.provenance.date.instant).generateSome)
      }
      val importedExternal = {
        val ds = datasetEntities(datasetProvenanceImportedExternal).generateOne
          .copy(parts = Nil)
          .to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]]
        ds.copy(maybeInvalidationTime = invalidationTimes(ds.provenance.date.instant).generateSome)
      }
      val ancestorInternal = {
        val ds = datasetEntities(datasetProvenanceImportedInternalAncestorInternal).generateOne
          .copy(parts = Nil)
          .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
        ds.copy(maybeInvalidationTime = invalidationTimes(ds.provenance.date.instant).generateSome)
      }
      val ancestorExternal = {
        val ds = datasetEntities(datasetProvenanceImportedInternalAncestorExternal).generateOne
          .copy(parts = Nil)
          .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
        ds.copy(maybeInvalidationTime = invalidationTimes(ds.provenance.date.instant).generateSome)
      }
      val modified = {
        val ds = datasetEntities(datasetProvenanceModified).generateOne
          .copy(parts = Nil)
          .to[entities.Dataset[entities.Dataset.Provenance.Modified]]
        ds.copy(maybeInvalidationTime = invalidationTimes(ds.provenance.date.instant).generateSome)
      }

      (() => projectMetadata.findInvalidatedDatasets)
        .expects()
        .returning(List(internal, importedExternal, ancestorInternal, ancestorExternal, modified))

      val internalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdates(_: entities.Dataset[entities.Dataset.Provenance.Internal])(_: Dataset.Provenance.Internal.type))
        .expects(internal, *)
        .returning(internalDsQueries)

      val importedExternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdates(_: entities.Dataset[entities.Dataset.Provenance.ImportedExternal])(
          _: Dataset.Provenance.ImportedExternal.type
        ))
        .expects(importedExternal, *)
        .returning(importedExternalDsQueries)

      val ancestorInternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdates(_: entities.Dataset[entities.Dataset.Provenance.ImportedInternal])(
          _: Dataset.Provenance.ImportedInternal.type
        ))
        .expects(ancestorInternal, *)
        .returning(ancestorInternalDsQueries)

      val ancestorExternalDsQueries = sparqlQueries.generateList()
      (updatesCreator
        .prepareUpdates(_: entities.Dataset[entities.Dataset.Provenance.ImportedInternal])(
          _: Dataset.Provenance.ImportedInternal.type
        ))
        .expects(ancestorExternal, *)
        .returning(ancestorExternalDsQueries)

      val step = transformer.createTransformationStep

      val Success(Right(updateResult)) = step.run(projectMetadata).value
      updateResult.projectMetadata shouldBe projectMetadata
      updateResult.queries         shouldBe (internalDsQueries ::: importedExternalDsQueries ::: ancestorInternalDsQueries ::: ancestorExternalDsQueries)

    }

    "return the ProcessingRecoverableFailure if calls to KG fails with a network or HTTP error" in new TestCase {
      val dataset = datasetEntities(datasetProvenanceImportedInternal).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]

      (() => projectMetadata.findInternallyImportedDatasets).expects().returning(List(dataset))

      val exception = recoverableClientErrors.generateOne
      findingParentTopmostSameAsFor(dataset.provenance.sameAs,
                                    returning = exception.raiseError[Try, Option[TopmostSameAs]]
      )

      val step = transformer.createTransformationStep

      val Success(Left(recoverableError)) = step.run(projectMetadata).value

      recoverableError            shouldBe a[TransformationRecoverableError]
      recoverableError.getMessage shouldBe "Problem finding dataset details in KG"
    }

    "fail with NonRecoverableFailure if finding calls to KG fails with an unknown exception" in new TestCase {
      val dataset = datasetEntities(datasetProvenanceImportedInternal).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]

      (() => projectMetadata.findInternallyImportedDatasets).expects().returning(List(dataset))

      val exception = exceptions.generateOne
      findingParentTopmostSameAsFor(dataset.provenance.sameAs,
                                    returning = exception.raiseError[Try, Option[TopmostSameAs]]
      )

      val step = transformer.createTransformationStep

      val Failure(nonRecoverableError) = step.run(projectMetadata).value
      nonRecoverableError shouldBe exception
    }

  }

  private trait TestCase {
    val kgDatasetInfoFinder = mock[KGDatasetInfoFinder[Try]]
    val updatesCreator      = mock[UpdatesCreator]
    val transformer         = new DatasetTransformerImpl[Try](kgDatasetInfoFinder, updatesCreator)

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
