/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.datasets

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.datasets.{InternalSameAs, ResourceId, TopmostDerivedFrom, TopmostSameAs}
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, persons}
import io.renku.rdfstore.SparqlQuery
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.ProjectFunctions
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.Queries
import io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.Generators.recoverableClientErrors
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

        val (dataset1 ::~ dataset2, project) = anyRenkuProjectEntities
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
        val updatedDataset1 = dataset1.update(parentTopmostSameAs)

        val dataset1CreatorUnlinkingQueries = givenRelevantCreatorsUnlinking(updatedDataset1)

        val dataset1CleanUpQueries = sparqlQueries.generateList()
        prepareQueriesForTopmostSameAsCleanup(updatedDataset1 -> parentTopmostSameAs.some,
                                              returning = dataset1CleanUpQueries
        )

        findingParentTopmostSameAsFor(dataset2.provenance.sameAs, returning = None.pure[Try])
        val dataset2KgTopmostSameAs = datasetTopmostSameAs.generateSome
        findingTopmostSameAsFor(dataset2.identification.resourceId, returning = dataset2KgTopmostSameAs.pure[Try])
        val dataset2Queries = sparqlQueries.generateList()
        prepareQueriesForSameAs(dataset2 -> dataset2KgTopmostSameAs, returning = dataset2Queries)

        val dataset2CreatorUnlinkingQueries = givenRelevantCreatorsUnlinking(dataset2)

        val dataset2CleanUpQueries = sparqlQueries.generateList()
        prepareQueriesForTopmostSameAsCleanup(dataset2 -> None, returning = dataset2CleanUpQueries)

        val step = transformer.createTransformationStep
        step.name.value shouldBe "Dataset Details Updates"

        val Success(Right((updatedProject, queries))) = (step run project).value
        updatedProject shouldBe ProjectFunctions.update(dataset1, updatedDataset1)(project)
        queries shouldBe Queries(
          preDataUploadQueries = dataset1CreatorUnlinkingQueries ::: dataset2CreatorUnlinkingQueries,
          postDataUploadQueries =
            dataset1Queries ::: dataset1CleanUpQueries ::: dataset2Queries ::: dataset2CleanUpQueries
        )
      }

    "update topmostDerivedFrom on all derivation hierarchies " +
      "and remove existing topmostDerivedFrom if different" in new TestCase {
        givenExternalCallsPass()

        def topmostDerivedFromFromDerivedFrom(
            otherDs: Dataset[Dataset.Provenance.Modified]
        ): Dataset[Dataset.Provenance.Modified] => Dataset[Dataset.Provenance.Modified] =
          ds => ds.copy(provenance = ds.provenance.copy(topmostDerivedFrom = TopmostDerivedFrom(otherDs.entityId)))

        val (_ ::~ modification1, project) =
          anyRenkuProjectEntities.addDatasetAndModification(datasetEntities(provenanceInternal)).generateOne
        val (d, projectUpdate2) = project.addDataset(
          modification1.createModification().modify(topmostDerivedFromFromDerivedFrom(modification1))
        )
        val finalProject = projectUpdate2.to[entities.Project]

        // at this stage topmostDerivedFrom should be the same as derivedFrom
        val topDS :: mod1DS :: mod2DS :: Nil = finalProject.datasets
        mod1DS.provenance.topmostDerivedFrom shouldBe TopmostDerivedFrom(topDS.identification.resourceId.show)
        mod2DS.provenance.topmostDerivedFrom shouldBe TopmostDerivedFrom(mod1DS.identification.resourceId.show)

        def dataset(expectedDS: entities.Dataset[entities.Dataset.Provenance]) = where {
          (ds: entities.Dataset[entities.Dataset.Provenance.Modified]) =>
            ds.identification.identifier == expectedDS.identification.identifier &&
            ds.provenance.topmostDerivedFrom == topDS.provenance.topmostDerivedFrom
        }
        val mod1DSQueries = sparqlQueries.generateList()
        (updatesCreator
          .deleteOtherTopmostDerivedFrom(_: entities.Dataset[entities.Dataset.Provenance.Modified]))
          .expects(dataset(mod1DS))
          .returning(mod1DSQueries)
        val mod2DSQueries = sparqlQueries.generateList()
        (updatesCreator
          .deleteOtherTopmostDerivedFrom(_: entities.Dataset[entities.Dataset.Provenance.Modified]))
          .expects(dataset(mod2DS))
          .returning(mod2DSQueries)

        val step = transformer.createTransformationStep

        val Success(Right((updatedProject, queries))) = (step run finalProject).value

        val updatedTopDS :: updatedMod1DS :: updatedMod2DS :: Nil = updatedProject.datasets
        updatedTopDS.provenance.topmostDerivedFrom  shouldBe topDS.provenance.topmostDerivedFrom
        updatedMod1DS.provenance.topmostDerivedFrom shouldBe updatedTopDS.provenance.topmostDerivedFrom
        updatedMod2DS.provenance.topmostDerivedFrom shouldBe updatedTopDS.provenance.topmostDerivedFrom

        queries.preDataUploadQueries shouldBe Nil
        queries.postDataUploadQueries  should contain theSameElementsAs (mod1DSQueries ::: mod2DSQueries)
      }

    "prepare updates for deleted datasets" in new TestCase {
      val (internal ::~ _ ::~ importedExternal ::~ _ ::~ ancestorInternal ::~ _ ::~ ancestorExternal ::~ _,
           projectWithAllDatasets
      ) = anyRenkuProjectEntities
        .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
        .addDatasetAndInvalidation(datasetEntities(provenanceImportedExternal))
        .addDatasetAndInvalidation(datasetEntities(provenanceImportedInternalAncestorInternal()))
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

      val creatorUnlinkingQueries = entitiesProjectWithAllDatasets.datasets >>= givenRelevantCreatorsUnlinking

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

      updatedProject               shouldBe entitiesProjectWithAllDatasets
      queries.preDataUploadQueries shouldBe creatorUnlinkingQueries
      queries.postDataUploadQueries should contain theSameElementsAs (internalDsQueries ::: importedExternalDsQueries ::: ancestorInternalDsQueries ::: ancestorExternalDsQueries)
    }

    "return the ProcessingRecoverableFailure if calls to KG fails with a network or HTTP error" in new TestCase {
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceImportedInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]], _.to[entities.Project])

      val exception = recoverableClientErrors.generateOne
      findingParentTopmostSameAsFor(dataset.provenance.sameAs,
                                    returning = exception.raiseError[Try, Option[TopmostSameAs]]
      )

      val step = transformer.createTransformationStep

      val Success(Left(recoverableError)) = step.run(project).value

      recoverableError          shouldBe a[ProcessingRecoverableError]
      recoverableError.getMessage should startWith("Problem finding dataset details in KG")
    }

    "fail with NonRecoverableFailure if finding calls to KG fails with an unknown exception" in new TestCase {
      val (dataset, project) = anyRenkuProjectEntities
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

    def givenRelevantCreatorsUnlinking(ds: entities.Dataset[entities.Dataset.Provenance]): List[SparqlQuery] = {
      val creatorsInKG = personResourceIds.generateSet()
      findingDatasetCreatorsFor(ds.identification.resourceId, returning = creatorsInKG.pure[Try])

      val unlinkingQueries = sparqlQueries.generateList()
      prepareQueriesUnlinkingRelevantCreators(ds, creatorsInKG, returning = unlinkingQueries)

      unlinkingQueries
    }

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

    def findingDatasetCreatorsFor(resourceId: ResourceId, returning: Try[Set[persons.ResourceId]]) =
      (kgDatasetInfoFinder
        .findDatasetCreators(_: ResourceId))
        .expects(resourceId)
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

    def prepareQueriesUnlinkingRelevantCreators(
        dataset:    entities.Dataset[entities.Dataset.Provenance],
        kgCreators: Set[persons.ResourceId],
        returning:  List[SparqlQuery]
    ) = (updatesCreator
      .queriesUnlinkingCreators(_: entities.Dataset[entities.Dataset.Provenance], _: Set[persons.ResourceId]))
      .expects(dataset, kgCreators)
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

    def givenExternalCallsPass() = {
      (kgDatasetInfoFinder.findDatasetCreators _)
        .expects(*)
        .returning(Set.empty[persons.ResourceId].pure[Try])
        .anyNumberOfTimes()

      (updatesCreator.queriesUnlinkingCreators _).expects(*, *).returning(List.empty).anyNumberOfTimes()
    }
  }
}
