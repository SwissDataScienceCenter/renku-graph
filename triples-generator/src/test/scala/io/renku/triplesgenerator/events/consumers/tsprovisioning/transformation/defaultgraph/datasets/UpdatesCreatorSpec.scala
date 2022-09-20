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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.defaultgraph.datasets

import cats.data.NonEmptyList
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.datasets.{SameAs, TopmostSameAs}
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{datasets, entities, persons}
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant
import scala.util.Random

class UpdatesCreatorSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with RenkuDataset
    with ScalaCheckPropertyChecks {

  "prepareUpdatesWhenInvalidated" should {

    "generate queries for deleted dataset which, " +
      "in case of internal dataset, " +
      "find datasets which have sameAs pointing to the deleted dataset " +
      "update their sameAs to None " +
      "select one the dataset with the oldest date " +
      "and update all datasets which have topmostSameAs pointing to the deleted DS with the selected resourceId" in {
        val grandparent  = datasetEntities(provenanceInternal).decoupledFromProject.generateOne.copy(parts = Nil)
        val (parent1, _) = anyRenkuProjectEntities.importDataset(grandparent).generateOne
        val (child1, _)  = anyRenkuProjectEntities.importDataset(parent1).generateOne
        val parent2 = {
          val (ds, _) = anyRenkuProjectEntities.importDataset(grandparent).generateOne
          provenanceLens[Dataset.Provenance.ImportedInternalAncestorInternal]
            .modify(_.copy(date = datasetCreatedDates(min = parent1.provenance.date.instant).generateOne))(ds)
        }
        val (child2, _) = anyRenkuProjectEntities.importDataset(parent2).generateOne

        val entitiesGrandparent = grandparent.to[entities.Dataset[entities.Dataset.Provenance.Internal]]
        val entitiesParent1 = parent1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
        val entitiesChild1  = child1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
        val entitiesParent2 = parent2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
        val entitiesChild2  = child2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]

        upload(to = renkuDataset, entitiesGrandparent, entitiesParent1, entitiesChild1, entitiesParent2, entitiesChild2)

        findDatasets.map(onlySameAsAndTop) shouldBe Set(
          (entitiesGrandparent.resourceId.value, None, entitiesGrandparent.provenance.topmostSameAs.value.some),
          (entitiesParent1.resourceId.value,
           entitiesGrandparent.resourceId.value.some,
           entitiesGrandparent.resourceId.value.some
          ),
          (entitiesChild1.resourceId.value,
           entitiesParent1.resourceId.value.some,
           entitiesGrandparent.resourceId.value.some
          ),
          (entitiesParent2.resourceId.value,
           entitiesGrandparent.resourceId.value.some,
           entitiesGrandparent.resourceId.value.some
          ),
          (entitiesChild2.resourceId.value,
           entitiesParent2.resourceId.value.some,
           entitiesGrandparent.resourceId.value.some
          )
        )

        UpdatesCreator.prepareUpdatesWhenInvalidated(entitiesGrandparent).runAll(on = renkuDataset).unsafeRunSync()

        findDatasets.map(onlySameAsAndTop) shouldBe Set(
          (entitiesGrandparent.resourceId.value, None, entitiesGrandparent.provenance.topmostSameAs.value.some),
          (entitiesParent1.resourceId.value, None, entitiesParent1.resourceId.value.some),
          (entitiesChild1.resourceId.value,
           entitiesParent1.resourceId.value.some,
           entitiesParent1.resourceId.value.some
          ),
          (entitiesParent2.resourceId.value, None, entitiesParent1.resourceId.value.some),
          (entitiesChild2.resourceId.value,
           entitiesParent2.resourceId.value.some,
           entitiesParent1.resourceId.value.some
          )
        )
      }

    "generate queries for deleted dataset which, " +
      "in case of imported external dataset, " +
      "find datasets which have sameAs pointing to the deleted dataset " +
      "update their sameAs to their topmostSameAs" in {
        val grandparent = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne.copy(parts = Nil)
        val (parent1, _) = anyRenkuProjectEntities.importDataset(grandparent).generateOne
        val (child1, _)  = anyRenkuProjectEntities.importDataset(parent1).generateOne
        val parent2 = {
          val (ds, _) = anyRenkuProjectEntities.importDataset(grandparent).generateOne
          provenanceLens[Dataset.Provenance.ImportedInternalAncestorExternal]
            .modify(_.copy(date = datasetPublishedDates(parent1.provenance.date).generateOne))(ds)
        }
        val (child2, _) = anyRenkuProjectEntities.importDataset(parent2).generateOne

        val entitiesGrandparent = grandparent.to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]]
        val entitiesParent1 = parent1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
        val entitiesChild1  = child1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
        val entitiesParent2 = parent2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
        val entitiesChild2  = child2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]

        upload(to = renkuDataset, entitiesGrandparent, entitiesParent1, entitiesChild1, entitiesParent2, entitiesChild2)

        findDatasets.map(onlySameAsAndTop) shouldBe Set(
          (entitiesGrandparent.resourceId.value,
           entitiesGrandparent.provenance.sameAs.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesParent1.resourceId.value,
           entitiesGrandparent.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesChild1.resourceId.value,
           entitiesParent1.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesParent2.resourceId.value,
           entitiesGrandparent.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesChild2.resourceId.value,
           entitiesParent2.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          )
        )

        UpdatesCreator.prepareUpdatesWhenInvalidated(entitiesGrandparent).runAll(on = renkuDataset).unsafeRunSync()

        findDatasets.map(onlySameAsAndTop) shouldBe Set(
          (entitiesGrandparent.resourceId.value,
           entitiesGrandparent.provenance.sameAs.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesParent1.resourceId.value,
           entitiesGrandparent.provenance.sameAs.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesChild1.resourceId.value,
           entitiesParent1.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesParent2.resourceId.value,
           entitiesGrandparent.provenance.sameAs.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesChild2.resourceId.value,
           entitiesParent2.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          )
        )
      }

    forAll {
      Table(
        ("Provenance name", "Parent Dataset and SameAs"),
        ("internal", {
           val d = datasetEntities(provenanceInternal).decoupledFromProject.generateOne.copy(parts = Nil)
           d -> Option.empty[SameAs]
         }
        ),
        ("imported external", {
           val d = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne.copy(parts = Nil)
           d -> d.provenance.sameAs.some
         }
        )
      )
    } { case (datasetType, (grandparent, maybeGrandparentSameAs)) =>
      "generate queries for deleted dataset which, " +
        s"in case of $datasetType dataset, " +
        "find datasets which have sameAs pointing to the deleted dataset " +
        "update their sameAs to the deleted dataset sameAs" in {
          val (parent, _) = anyRenkuProjectEntities.importDataset(grandparent)(importedInternal).generateOne
          val (child1, _) = anyRenkuProjectEntities.importDataset(parent)(importedInternal).generateOne
          val (child2, _) = anyRenkuProjectEntities.importDataset(parent)(importedInternal).generateOne

          val entitiesGrandparent =
            grandparent.widen[Dataset.Provenance].to[entities.Dataset[entities.Dataset.Provenance]]
          val entitiesParent = parent
            .widen[Dataset.Provenance.ImportedInternal]
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          val entitiesChild1 = child1
            .widen[Dataset.Provenance.ImportedInternal]
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          val entitiesChild2 = child2
            .widen[Dataset.Provenance.ImportedInternal]
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]

          val grandparentTopmostSameAs = entitiesGrandparent.provenance.topmostSameAs.value.some

          upload(to = renkuDataset, entitiesGrandparent, entitiesParent, entitiesChild1, entitiesChild2)

          findDatasets.map(onlySameAsAndTop) shouldBe Set(
            (entitiesGrandparent.resourceId.value, maybeGrandparentSameAs.map(_.value), grandparentTopmostSameAs),
            (entitiesParent.resourceId.value, entitiesGrandparent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild1.resourceId.value, entitiesParent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild2.resourceId.value, entitiesParent.resourceId.value.some, grandparentTopmostSameAs)
          )

          UpdatesCreator.prepareUpdatesWhenInvalidated(entitiesParent).runAll(on = renkuDataset).unsafeRunSync()

          findDatasets.map(onlySameAsAndTop) shouldBe Set(
            (entitiesGrandparent.resourceId.value, maybeGrandparentSameAs.map(_.value), grandparentTopmostSameAs),
            (entitiesParent.resourceId.value, entitiesGrandparent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild1.resourceId.value, entitiesGrandparent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild2.resourceId.value, entitiesGrandparent.resourceId.value.some, grandparentTopmostSameAs)
          )
        }
    }
  }

  "prepareUpdates" should {

    "generate queries which " +
      "updates topmostSameAs for all datasets where topmostSameAs points to the current dataset" +
      "when the topmostSameAs on the current dataset is different than the value in KG" in {
        val dataset0AsTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value)

        val dataset1 = {
          val ds = datasetEntities(provenanceImportedInternal).decoupledFromProject.generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          setTopmostSameAs(ds, dataset0AsTopmostSameAs)
        }
        val dataset2 = {
          val ds = datasetEntities(provenanceImportedInternal).decoupledFromProject.generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          setTopmostSameAs(ds, TopmostSameAs(dataset1.resourceId.value))
        }

        upload(to = renkuDataset, dataset2)

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (dataset2.resourceId.value, TopmostSameAs(dataset1.resourceId.value).value.some)
        )

        UpdatesCreator.prepareUpdates(dataset1, Set.empty).runAll(on = renkuDataset).unsafeRunSync()

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (dataset2.resourceId.value, dataset0AsTopmostSameAs.value.some)
        )
      }

    "generate queries which " +
      "updates topmostSameAs for all datasets where topmostSameAs points to the current dataset" +
      "when the topmostSameAs on the current dataset is different than the value in KG -" +
      "case when some datasets have multiple topmostSameAs" in {
        val dataset0AsTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value)

        val dataset1 = {
          val ds = datasetEntities(provenanceImportedInternal).decoupledFromProject.generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          setTopmostSameAs(ds, dataset0AsTopmostSameAs)
        }
        val dataset2 = {
          val ds = datasetEntities(provenanceImportedInternal).decoupledFromProject.generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          setTopmostSameAs(ds, TopmostSameAs(dataset1.resourceId.value))
        }

        upload(to = renkuDataset, dataset2)

        val otherTopmostSameAs = datasetTopmostSameAs.generateOne
        insert(to = renkuDataset, Triple.edge(dataset2.resourceId, renku / "topmostSameAs", otherTopmostSameAs))

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (dataset2.resourceId.value, TopmostSameAs(dataset1.resourceId.value).value.some),
          (dataset2.resourceId.value, otherTopmostSameAs.value.some)
        )

        UpdatesCreator.prepareUpdates(dataset1, Set.empty).runAll(on = renkuDataset).unsafeRunSync()

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (dataset2.resourceId.value, dataset0AsTopmostSameAs.value.some)
        )
      }

    "generate no queries " +
      "if the topmostSameAs on the current DS is the only topmostSameAs for that DS in KG" in {

        val ds = datasetEntities(provenanceImportedInternal).decoupledFromProject.generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]

        UpdatesCreator.prepareUpdates(ds, Set(ds.provenance.topmostSameAs)) shouldBe Nil
      }
  }

  "prepareTopmostSameAsCleanup" should {

    "return no updates if there is no parent TopmostSameAs given" in {
      UpdatesCreator
        .prepareTopmostSameAsCleanup(
          datasetEntities(provenanceImportedInternal).decoupledFromProject.generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]],
          maybeParentTopmostSameAs = None
        ) shouldBe Nil
    }

    "return updates deleting additional TopmostSameAs if there is parent TopmostSameAs given" in {
      val dataset = datasetEntities(provenanceImportedInternal).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]

      upload(to = renkuDataset, dataset)

      // simulate DS having two topmostSameAs
      val parentTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value)
      upload(to = renkuDataset, setTopmostSameAs(dataset, parentTopmostSameAs))

      findDatasets.map(onlyTopmostSameAs) shouldBe Set(
        (dataset.resourceId.value, dataset.provenance.topmostSameAs.value.some),
        (dataset.resourceId.value, parentTopmostSameAs.value.some)
      )

      UpdatesCreator
        .prepareTopmostSameAsCleanup(dataset, maybeParentTopmostSameAs = parentTopmostSameAs.some)
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findDatasets.map(onlyTopmostSameAs) shouldBe Set(
        (dataset.resourceId.value, parentTopmostSameAs.value.some)
      )
    }

    "return updates not deleting sole TopmostSameAs if there is parent TopmostSameAs given" in {
      val dataset = datasetEntities(provenanceImportedInternal).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]

      upload(to = renkuDataset, dataset)

      findDatasets.map(onlyTopmostSameAs) shouldBe Set(
        (dataset.resourceId.value, dataset.provenance.topmostSameAs.value.some)
      )

      UpdatesCreator
        .prepareTopmostSameAsCleanup(dataset,
                                     maybeParentTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value).some
        )
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findDatasets.map(onlyTopmostSameAs) shouldBe Set(
        (dataset.resourceId.value, dataset.provenance.topmostSameAs.value.some)
      )
    }
  }

  "queriesUnlinkingCreators" should {

    "prepare delete queries for all dataset creators existing in KG but not in the model" in {
      forAll(
        datasetEntities(provenanceNonModified)
          .modify(provenanceLens.modify(creatorsLens.modify(_ => personEntities.generateNonEmptyList())))
          .decoupledFromProject
      ) { kgDataset =>
        val kgDatasetEntities = kgDataset.to[entities.Dataset[entities.Dataset.Provenance]]

        upload(to = renkuDataset, kgDatasetEntities)

        val creators = kgDataset.provenance.creators
        findCreators(kgDatasetEntities.resourceId) shouldBe creators.map(_.resourceId).toList.toSet

        val someCreator        = Random.shuffle(creators.toList).head
        val creatorsNotChanged = creators.filterNot(_ == someCreator)
        val newCreators =
          NonEmptyList.fromListUnsafe(creatorsNotChanged ::: personEntities.generateNonEmptyList().toList)
        val model = provenanceLens[Dataset.Provenance.NonModified]
          .modify(creatorsLens.modify(_ => newCreators))(kgDataset)
          .to[entities.Dataset[entities.Dataset.Provenance]]

        UpdatesCreator
          .queriesUnlinkingCreators(model, creators.map(_.resourceId).toList.toSet)
          .runAll(on = renkuDataset)
          .unsafeRunSync()

        findCreators(kgDatasetEntities.resourceId) shouldBe creatorsNotChanged.map(_.resourceId).toSet
      }
    }

    "prepare no queries if there's no change in DS creators" in {
      val ds = datasetEntities(provenanceNonModified)
        .modify(provenanceLens.modify(creatorsLens.modify(_ => personEntities.generateNonEmptyList())))
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      UpdatesCreator.queriesUnlinkingCreators(ds, ds.provenance.creators.map(_.resourceId).toList.toSet) shouldBe Nil
    }
  }

  "removeOtherOriginalIdentifiers" should {

    "prepare queries that removes all additional originalIdentifier triples on the DS" in {
      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      upload(to = renkuDataset, ds)

      val existingOriginalId1 = datasetOriginalIdentifiers.generateOne
      insert(to = renkuDataset, Triple(ds.resourceId, renku / "originalIdentifier", existingOriginalId1))
      val existingOriginalId2 = datasetOriginalIdentifiers.generateOne
      insert(to = renkuDataset, Triple(ds.resourceId, renku / "originalIdentifier", existingOriginalId2))

      findOriginalIdentifiers(ds.identification.identifier) shouldBe Set(ds.provenance.originalIdentifier,
                                                                         existingOriginalId1,
                                                                         existingOriginalId2
      )

      UpdatesCreator
        .removeOtherOriginalIdentifiers(ds, Set(existingOriginalId1, existingOriginalId2))
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findOriginalIdentifiers(ds.identification.identifier) shouldBe Set(ds.provenance.originalIdentifier)
    }

    "prepare no queries if there's only the correct originalIdentifier for the DS in KG" in {
      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      UpdatesCreator.removeOtherOriginalIdentifiers(ds, Set(ds.provenance.originalIdentifier)) shouldBe Nil
    }

    "prepare no queries if there's no originalIdentifier for the DS in KG" in {
      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      UpdatesCreator.removeOtherOriginalIdentifiers(ds, Set.empty) shouldBe Nil
    }
  }

  "removeOtherDSDateCreated" should {

    "prepare queries that removes all additional dateCreated triples on the DS" in {
      val ds = datasetEntities(provenanceInternal).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Internal]]

      upload(to = renkuDataset, ds)

      val existingDateCreated1 = datasetCreatedDates(min = ds.provenance.date.instant).generateOne
      insert(to = renkuDataset, Triple(ds.resourceId, schema / "dateCreated", existingDateCreated1))
      val existingDateCreated2 = datasetCreatedDates(min = ds.provenance.date.instant).generateOne
      insert(to = renkuDataset, Triple(ds.resourceId, schema / "dateCreated", existingDateCreated2))

      findDateCreated(ds.identification.identifier) shouldBe Set(ds.provenance.date,
                                                                 existingDateCreated1,
                                                                 existingDateCreated2
      )

      UpdatesCreator
        .removeOtherDateCreated(ds, Set(existingDateCreated1, existingDateCreated2))
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findDateCreated(ds.identification.identifier) shouldBe Set(ds.provenance.date)
    }

    "prepare no queries if there's only the correct dateCreated for the DS in KG" in {
      val ds = datasetEntities(provenanceInternal).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Internal]]

      UpdatesCreator.removeOtherDateCreated(ds, Set(ds.provenance.date)) shouldBe Nil
    }

    "prepare no queries if it's a DS with datePublished" in {
      val ds = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]]

      UpdatesCreator.removeOtherDateCreated(ds, Set.empty) shouldBe Nil
    }

    "prepare no queries if there's no dateCreated for the DS in KG" in {
      val ds = datasetEntities(provenanceInternal).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Internal]]

      UpdatesCreator.removeOtherDateCreated(ds, Set.empty) shouldBe Nil
    }
  }

  "removeOtherDescriptions" should {

    "prepare queries that removes all additional description triples on the DS" in {
      val description1 = datasetDescriptions.generateOne
      val ds = datasetEntities(provenanceNonModified)
        .modify(replaceDSDesc(description1.some))
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      upload(to = renkuDataset, ds)

      val description2 = datasetDescriptions.generateOne
      insert(to = renkuDataset, Triple(ds.resourceId, schema / "description", description2))
      val description3 = datasetDescriptions.generateOne
      insert(to = renkuDataset, Triple(ds.resourceId, schema / "description", description3))

      findDescriptions(ds.identification.identifier) shouldBe Set(description1, description3, description2)

      UpdatesCreator
        .removeOtherDescriptions(ds, Set(description2, description3))
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findDescriptions(ds.identification.identifier) shouldBe Set(description1)
    }

    "prepare queries that removes all description triples on the DS if there's no description on DS but some in KG" in {
      val ds = datasetEntities(provenanceNonModified)
        .modify(replaceDSDesc(None))
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      upload(to = renkuDataset, ds)

      val description = datasetDescriptions.generateOne
      insert(to = renkuDataset, Triple(ds.resourceId, schema / "description", description))

      findDescriptions(ds.identification.identifier) shouldBe Set(description)

      UpdatesCreator.removeOtherDescriptions(ds, Set(description)).runAll(on = renkuDataset).unsafeRunSync()

      findDescriptions(ds.identification.identifier) shouldBe Set.empty
    }

    "prepare no queries if there's no description on DS and in KG" in {
      val ds = datasetEntities(provenanceNonModified)
        .modify(replaceDSDesc(None))
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      UpdatesCreator.removeOtherDescriptions(ds, Set.empty) shouldBe Nil
    }

    "prepare no queries if there's only the correct description for the DS in KG" in {
      val description = datasetDescriptions.generateOne
      val ds = datasetEntities(provenanceNonModified)
        .modify(replaceDSDesc(description.some))
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      UpdatesCreator.removeOtherDescriptions(ds, Set(description)) shouldBe Nil
    }

    "prepare no queries if there's no description for the DS in KG" in {
      val ds = datasetEntities(provenanceNonModified)
        .modify(replaceDSDesc(datasetDescriptions.generateSome))
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      UpdatesCreator.removeOtherDescriptions(ds, Set.empty) shouldBe Nil
    }
  }

  "removeOtherDSSameAs" should {

    "prepare queries that removes all additional Internal SameAs triples on the DS" in {

      val originalDS = datasetEntities(provenanceInternal).decoupledFromProject.generateOne
      val importedDS = datasetEntities(
        provenanceImportedInternalAncestorInternal(fixed(SameAs(originalDS.entityId)))
      ).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]

      upload(to = renkuDataset, originalDS.asJsonLD, importedDS.asJsonLD)

      val otherSameAs = datasetSameAs.generateOne.entityId
      insert(to = renkuDataset, Triple.edge(importedDS.resourceId, schema / "sameAs", otherSameAs))

      findSameAs(importedDS.identification.identifier).map(_.show) shouldBe Set(importedDS.provenance.sameAs.entityId,
                                                                                otherSameAs
      ).map(_.show)

      UpdatesCreator
        .removeOtherSameAs(importedDS, Set(SameAs(otherSameAs)))
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findSameAs(importedDS.identification.identifier).map(_.show) shouldBe Set(
        importedDS.provenance.sameAs.entityId.show
      )
    }

    "prepare queries that removes all additional External SameAs triples on the DS" in {

      val importedExternalDS = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]]

      upload(to = renkuDataset, importedExternalDS.asJsonLD)

      val otherSameAs = datasetSameAs.generateOne.entityId
      insert(to = renkuDataset, Triple.edge(importedExternalDS.resourceId, schema / "sameAs", otherSameAs))

      findSameAs(importedExternalDS.identification.identifier)
        .map(_.show) shouldBe Set(importedExternalDS.provenance.sameAs.entityId, otherSameAs).map(_.show)

      UpdatesCreator
        .removeOtherSameAs(importedExternalDS, Set(SameAs(otherSameAs)))
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findSameAs(importedExternalDS.identification.identifier).map(_.show) shouldBe Set(
        importedExternalDS.provenance.sameAs.entityId.show
      )
    }

    "prepare no queries if there's only the correct sameAs for the DS in KG" in {
      val importedDS = datasetEntities(
        provenanceImportedInternalAncestorInternal(datasetInternalSameAs.generateOne)
      ).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]

      UpdatesCreator.removeOtherSameAs(importedDS, Set(SameAs(importedDS.provenance.sameAs.entityId))) shouldBe Nil
    }

    "prepare no queries if there's no sameAs for the DS in KG" in {
      val importedDS = datasetEntities(
        provenanceImportedInternalAncestorInternal(datasetInternalSameAs.generateOne)
      ).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]

      UpdatesCreator.removeOtherSameAs(importedDS, Set.empty) shouldBe Nil
    }
  }

  "deleteOtherDerivedFrom" should {

    "prepare queries that removes other wasDerivedFrom from the given DS" in {
      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .createModification()
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Modified]]

      upload(to = renkuDataset, ds)

      val otherDerivedFrom       = datasetDerivedFroms.generateOne
      val otherDerivedFromJsonLD = otherDerivedFrom.asJsonLD
      upload(to = renkuDataset, otherDerivedFromJsonLD)
      val derivedFromId = otherDerivedFromJsonLD.entityId.getOrElse(fail(" Cannot obtain EntityId for DerivedFrom"))
      insert(to = renkuDataset, Triple.edge(ds.resourceId, prov / "wasDerivedFrom", derivedFromId))

      findDerivedFrom(ds.identification.identifier) shouldBe Set(ds.provenance.derivedFrom, otherDerivedFrom)

      UpdatesCreator.deleteOtherDerivedFrom(ds).runAll(on = renkuDataset).unsafeRunSync()

      findDerivedFrom(ds.identification.identifier) shouldBe Set(ds.provenance.derivedFrom)
    }
  }

  "deleteOtherTopmostDerivedFrom" should {

    "prepare queries that removes other topmostDerivedFrom from the given DS" in {
      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .createModification()
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Modified]]

      upload(to = renkuDataset, ds)

      val otherTopmostDerivedFrom = datasetTopmostDerivedFroms.generateOne
      insert(to = renkuDataset, Triple.edge(ds.resourceId, renku / "topmostDerivedFrom", otherTopmostDerivedFrom))

      findTopmostDerivedFrom(ds.identification.identifier) shouldBe Set(ds.provenance.topmostDerivedFrom,
                                                                        otherTopmostDerivedFrom
      )

      UpdatesCreator.deleteOtherTopmostDerivedFrom(ds).runAll(on = renkuDataset).unsafeRunSync()

      findTopmostDerivedFrom(ds.identification.identifier) shouldBe Set(ds.provenance.topmostDerivedFrom)
    }
  }

  private def setTopmostSameAs[P <: entities.Dataset.Provenance.ImportedInternal](dataset:       entities.Dataset[P],
                                                                                  topmostSameAs: TopmostSameAs
  ) = {
    val newProvenance = dataset.provenance match {
      case p: entities.Dataset.Provenance.ImportedInternalAncestorInternal => p.copy(topmostSameAs = topmostSameAs)
      case p: entities.Dataset.Provenance.ImportedInternalAncestorExternal => p.copy(topmostSameAs = topmostSameAs)
    }
    dataset.copy(provenance = newProvenance)
  }

  private def findDatasets: Set[(String, Option[String], Option[String], Option[String], Option[String])] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch ds data",
        Prefixes.of(prov -> "prov", renku -> "renku", schema -> "schema"),
        s"""|SELECT ?id ?maybeSameAs ?maybeTopmostSameAs ?maybeDerivedFrom ?maybeTopmostDerivedFrom
            |WHERE {
            |  ?id a schema:Dataset .
            |  OPTIONAL { ?id schema:sameAs/schema:url ?maybeSameAs } .
            |  OPTIONAL { ?id renku:topmostSameAs ?maybeTopmostSameAs } .
            |  OPTIONAL { ?id prov:wasDerivedFrom/schema:url ?maybeDerivedFrom } .
            |  OPTIONAL { ?id renku:topmostDerivedFrom ?maybeTopmostDerivedFrom } .
            |}
            |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row =>
        (row("id"),
         row.get("maybeSameAs"),
         row.get("maybeTopmostSameAs"),
         row.get("maybeDerivedFrom"),
         row.get("maybeTopmostDerivedFrom")
        )
      )
      .toSet

  private lazy val onlyTopmostSameAs
      : ((String, Option[String], Option[String], Option[String], Option[String])) => (String, Option[String]) = {
    case (resourceId, _, maybeTopmostSameAs, _, _) => resourceId -> maybeTopmostSameAs
  }

  private lazy val onlySameAsAndTop: (
      (String, Option[String], Option[String], Option[String], Option[String])
  ) => (String, Option[String], Option[String]) = { case (resourceId, sameAs, maybeTopmostSameAs, _, _) =>
    (resourceId, sameAs, maybeTopmostSameAs)
  }

  private def findCreators(resourceId: datasets.ResourceId): Set[persons.ResourceId] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch ds creator",
        Prefixes of schema -> "schema",
        s"""|SELECT ?personId
            |WHERE {
            |  ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                    schema:creator ?personId
            |}
            |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => persons.ResourceId.from(row("personId")))
      .sequence
      .fold(throw _, identity)
      .toSet

  private def findOriginalIdentifiers(id: datasets.Identifier): Set[datasets.OriginalIdentifier] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch ds originalIdentifier",
        Prefixes.of(renku -> "renku", schema -> "schema"),
        s"""|SELECT ?originalId
            |WHERE { 
            |  ?id a schema:Dataset;
            |      schema:identifier '$id';
            |      renku:originalIdentifier ?originalId.
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.OriginalIdentifier(row("originalId")))
      .toSet

  private def findDateCreated(id: datasets.Identifier): Set[datasets.DateCreated] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch ds date",
        Prefixes of schema -> "schema",
        s"""|SELECT ?date
            |WHERE { 
            |  ?id a schema:Dataset;
            |      schema:identifier '$id';
            |      schema:dateCreated ?date.
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.DateCreated(Instant.parse(row("date"))))
      .toSet

  private def findDescriptions(id: datasets.Identifier): Set[datasets.Description] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch ds description",
        Prefixes of schema -> "schema",
        s"""|SELECT ?desc
            |WHERE { 
            |  ?id a schema:Dataset;
            |      schema:identifier '$id';
            |      schema:description ?desc.
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.Description(row("desc")))
      .toSet

  private def findSameAs(id: datasets.Identifier): Set[datasets.SameAs] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch sameAs",
        Prefixes of schema -> "schema",
        s"""|SELECT ?sameAs
            |WHERE { 
            |  ?id a schema:Dataset;
            |      schema:identifier '$id';
            |      schema:sameAs ?sameAs.
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.SameAs(row("sameAs")))
      .toSet

  private def findTopmostDerivedFrom(id: datasets.Identifier): Set[datasets.TopmostDerivedFrom] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch topmostDerivedFrom",
        Prefixes.of(renku -> "renku", schema -> "schema"),
        s"""|SELECT ?topmostDerivedFrom
            |WHERE { 
            |  ?id a schema:Dataset;
            |      schema:identifier '$id';
            |      renku:topmostDerivedFrom ?topmostDerivedFrom
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.TopmostDerivedFrom(row("topmostDerivedFrom")))
      .toSet

  private def findDerivedFrom(id: datasets.Identifier): Set[datasets.DerivedFrom] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch derivedFrom",
        Prefixes.of(prov -> "prov", schema -> "schema"),
        s"""|SELECT ?derivedFrom
            |WHERE {
            |  ?id a schema:Dataset;
            |      schema:identifier '$id';
            |      prov:wasDerivedFrom/schema:url ?derivedFrom
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.DerivedFrom(row("derivedFrom")))
      .toSet

  private implicit class SameAsOps(sameAs: SameAs) {
    lazy val entityId: EntityId = sameAs.asJsonLD.entityId.getOrElse(fail("Cannot obtain sameAs @id"))
  }
}
