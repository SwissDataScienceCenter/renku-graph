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

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.datasets.{SameAs, TopmostSameAs}
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{datasets, entities, persons}
import io.renku.jsonld.syntax._
import io.renku.rdfstore.InMemoryRdfStore
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class UpdatesCreatorSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryRdfStore
    with should.Matchers
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

        loadToStore(entitiesGrandparent, entitiesParent1, entitiesChild1, entitiesParent2, entitiesChild2)

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

        UpdatesCreator.prepareUpdatesWhenInvalidated(entitiesGrandparent).runAll.unsafeRunSync()

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

        loadToStore(entitiesGrandparent, entitiesParent1, entitiesChild1, entitiesParent2, entitiesChild2)

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

        UpdatesCreator.prepareUpdatesWhenInvalidated(entitiesGrandparent).runAll.unsafeRunSync()

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

          loadToStore(entitiesGrandparent, entitiesParent, entitiesChild1, entitiesChild2)

          findDatasets.map(onlySameAsAndTop) shouldBe Set(
            (entitiesGrandparent.resourceId.value, maybeGrandparentSameAs.map(_.value), grandparentTopmostSameAs),
            (entitiesParent.resourceId.value, entitiesGrandparent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild1.resourceId.value, entitiesParent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild2.resourceId.value, entitiesParent.resourceId.value.some, grandparentTopmostSameAs)
          )

          UpdatesCreator.prepareUpdatesWhenInvalidated(entitiesParent).runAll.unsafeRunSync()

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
      "updates topmostSameAs for all datasets whose topmostSameAs points to the current dataset" +
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

        loadToStore(dataset2)

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (dataset2.resourceId.value, TopmostSameAs(dataset1.resourceId.value).value.some)
        )

        UpdatesCreator.prepareUpdates(dataset1, None).runAll.unsafeRunSync()

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (dataset2.resourceId.value, dataset0AsTopmostSameAs.value.some)
        )
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

      loadToStore(dataset)

      // simulate DS having two topmostSameAs
      val parentTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value)
      loadToStore(setTopmostSameAs(dataset, parentTopmostSameAs))

      findDatasets.map(onlyTopmostSameAs) shouldBe Set(
        (dataset.resourceId.value, dataset.provenance.topmostSameAs.value.some),
        (dataset.resourceId.value, parentTopmostSameAs.value.some)
      )

      UpdatesCreator
        .prepareTopmostSameAsCleanup(dataset, maybeParentTopmostSameAs = parentTopmostSameAs.some)
        .runAll
        .unsafeRunSync()

      findDatasets.map(onlyTopmostSameAs) shouldBe Set(
        (dataset.resourceId.value, parentTopmostSameAs.value.some)
      )
    }

    "return updates not deleting sole TopmostSameAs if there is parent TopmostSameAs given" in {
      val dataset = datasetEntities(provenanceImportedInternal).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]

      loadToStore(dataset)

      findDatasets.map(onlyTopmostSameAs) shouldBe Set(
        (dataset.resourceId.value, dataset.provenance.topmostSameAs.value.some)
      )

      UpdatesCreator
        .prepareTopmostSameAsCleanup(dataset,
                                     maybeParentTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value).some
        )
        .runAll
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

        loadToStore(kgDatasetEntities)

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
          .runAll
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

  "removeOtherInitialVersions" should {

    "prepare queries that removes all additional originalIdentifier triples on the DS" in {
      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      loadToStore(ds)

      val existingInitialVersion1 = datasetInitialVersions.generateOne
      insertTriple(ds.resourceId, "renku:originalIdentifier", s"'$existingInitialVersion1'")
      val existingInitialVersion2 = datasetInitialVersions.generateOne
      insertTriple(ds.resourceId, "renku:originalIdentifier", s"'$existingInitialVersion2'")

      findInitialVersions(ds.identification.identifier) shouldBe Set(ds.provenance.initialVersion,
                                                                     existingInitialVersion1,
                                                                     existingInitialVersion2
      )

      UpdatesCreator
        .removeOtherInitialVersions(ds, Set(existingInitialVersion1, existingInitialVersion2))
        .runAll
        .unsafeRunSync()

      findInitialVersions(ds.identification.identifier) shouldBe Set(ds.provenance.initialVersion)
    }

    "prepare no queries if there's only the correct originalIdentifier for the DS in KG" in {
      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      UpdatesCreator.removeOtherInitialVersions(ds, Set(ds.provenance.initialVersion)) shouldBe Nil
    }

    "prepare no queries if there's no originalIdentifier for the DS in KG" in {
      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      UpdatesCreator.removeOtherInitialVersions(ds, Set.empty) shouldBe Nil
    }
  }

  "deleteOtherDerivedFrom" should {

    "prepare queries that removes other wasDerivedFrom from the given DS" in {
      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .createModification()
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Modified]]

      loadToStore(ds)

      val otherDerivedFrom       = datasetDerivedFroms.generateOne
      val otherDerivedFromJsonLD = otherDerivedFrom.asJsonLD
      loadToStore(otherDerivedFromJsonLD)
      val derivedFromId = otherDerivedFromJsonLD.entityId.getOrElse(fail(" Cannot obtain EntityId for DerivedFrom"))
      insertTriple(ds.resourceId, "prov:wasDerivedFrom", s"<$derivedFromId>")

      findDerivedFrom(ds.identification.identifier) shouldBe Set(ds.provenance.derivedFrom, otherDerivedFrom)

      UpdatesCreator.deleteOtherDerivedFrom(ds).runAll.unsafeRunSync()

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

      loadToStore(ds)

      val otherTopmostDerivedFrom = datasetTopmostDerivedFroms.generateOne
      insertTriple(ds.resourceId, "renku:topmostDerivedFrom", otherTopmostDerivedFrom.showAs[RdfResource])

      findTopmostDerivedFrom(ds.identification.identifier) shouldBe Set(ds.provenance.topmostDerivedFrom,
                                                                        otherTopmostDerivedFrom
      )

      UpdatesCreator.deleteOtherTopmostDerivedFrom(ds).runAll.unsafeRunSync()

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
    runQuery(s"""|SELECT ?id ?maybeSameAs ?maybeTopmostSameAs ?maybeDerivedFrom ?maybeTopmostDerivedFrom
                 |WHERE {
                 |  ?id a schema:Dataset .
                 |  OPTIONAL { ?id schema:sameAs/schema:url ?maybeSameAs } .
                 |  OPTIONAL { ?id renku:topmostSameAs ?maybeTopmostSameAs } .
                 |  OPTIONAL { ?id prov:wasDerivedFrom/schema:url ?maybeDerivedFrom } .
                 |  OPTIONAL { ?id renku:topmostDerivedFrom ?maybeTopmostDerivedFrom } .
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
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
    runQuery(s"""|SELECT ?personId
                 |WHERE {
                 |  ${resourceId.showAs[RdfResource]} a schema:Dataset;
                 |                                    schema:creator ?personId
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => persons.ResourceId.from(row("personId")))
      .sequence
      .fold(throw _, identity)
      .toSet

  private def findInitialVersions(id: datasets.Identifier): Set[datasets.InitialVersion] =
    runQuery(s"""|SELECT ?initial 
                 |WHERE { 
                 |  ?id a schema:Dataset;
                 |      schema:identifier '$id';
                 |      renku:originalIdentifier ?initial.
                 |}""".stripMargin)
      .unsafeRunSync()
      .map(row => datasets.InitialVersion(row("initial")))
      .toSet

  private def findTopmostDerivedFrom(id: datasets.Identifier): Set[datasets.TopmostDerivedFrom] =
    runQuery(s"""|SELECT ?topmostDerivedFrom 
                 |WHERE { 
                 |  ?id a schema:Dataset;
                 |      schema:identifier '$id';
                 |      renku:topmostDerivedFrom ?topmostDerivedFrom
                 |}""".stripMargin)
      .unsafeRunSync()
      .map(row => datasets.TopmostDerivedFrom(row("topmostDerivedFrom")))
      .toSet

  private def findDerivedFrom(id: datasets.Identifier): Set[datasets.DerivedFrom] =
    runQuery(s"""|SELECT ?derivedFrom 
                 |WHERE { 
                 |  ?id a schema:Dataset;
                 |      schema:identifier '$id';
                 |      prov:wasDerivedFrom/schema:url ?derivedFrom
                 |}""".stripMargin)
      .unsafeRunSync()
      .map(row => datasets.DerivedFrom(row("derivedFrom")))
      .toSet
}
