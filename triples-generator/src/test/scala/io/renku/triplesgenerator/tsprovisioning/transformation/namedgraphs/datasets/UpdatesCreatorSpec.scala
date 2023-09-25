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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.datasets

import cats.data.NonEmptyList
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model._
import io.renku.graph.model.datasets.{SameAs, TopmostSameAs}
import io.renku.graph.model.entities.Dataset.Provenance
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.testentities.{Dataset, ModelOps}
import io.renku.graph.model.views.RdfResource
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, NamedGraph}
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant
import scala.util.Random

class UpdatesCreatorSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with EntitiesGenerators
    with ModelOps
    with InMemoryJenaForSpec
    with ProjectsDataset
    with ScalaCheckPropertyChecks {

  "prepareUpdatesWhenInvalidated" should {

    "generate queries for deleted dataset which, " +
      "in case of internal dataset, " +
      "find datasets which have sameAs pointing to the deleted dataset " +
      "update their sameAs to None " +
      "select dataset with the oldest date " +
      "and update all datasets which have topmostSameAs pointing to the deleted DS with the selected resourceId" in {
        val (grandparent, grandparentProject) = anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceInternal))
          .generateOne
          .leftMap(_.copy(parts = Nil))

        val (parent1, parent1Project) = anyRenkuProjectEntities.importDataset(grandparent).generateOne
        val (parent2, parent2Project) =
          anyRenkuProjectEntities
            .importDataset(grandparent)
            .map { case (ds, proj) =>
              val updatedDS = ds.modifyProvenance(
                _.copy(date = datasetCreatedDates(min = parent1.provenance.date.instant).generateOne)
              )
              updatedDS -> proj.replaceDatasets(updatedDS)
            }
            .generateOne
        val (child2, child2Project) = anyRenkuProjectEntities.importDataset(parent2).generateOne

        val entitiesGrandparent = grandparent.to[entities.Dataset[entities.Dataset.Provenance.Internal]]
        val entitiesParent1 = parent1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
        val entitiesParent2 = parent2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
        val entitiesChild2  = child2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]

        upload(to = projectsDataset, grandparentProject, parent1Project, parent2Project, child2Project)

        findDatasets.map(onlySameAsAndTop) shouldBe Set(
          (entitiesGrandparent.resourceId.value, None, entitiesGrandparent.provenance.topmostSameAs.value.some),
          (entitiesParent1.resourceId.value,
           entitiesGrandparent.resourceId.value.some,
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

        UpdatesCreator.prepareUpdatesWhenInvalidated(entitiesGrandparent).runAll(on = projectsDataset).unsafeRunSync()

        findDatasets.map(onlySameAsAndTop) shouldBe Set(
          (entitiesGrandparent.resourceId.value, None, entitiesGrandparent.provenance.topmostSameAs.value.some),
          (entitiesParent1.resourceId.value, None, entitiesParent1.resourceId.value.some),
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
        val (grandparent, grandparentProject) = anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceImportedExternal))
          .generateOne
          .leftMap(_.copy(parts = Nil))
        val (parent1, parent1Project) = anyRenkuProjectEntities.importDataset(grandparent).generateOne
        val (child1, child1Project)   = anyRenkuProjectEntities.importDataset(parent1).generateOne
        val (parent2, parent2Project) =
          anyRenkuProjectEntities
            .importDataset(grandparent)
            .map { case (ds, proj) =>
              val updatedDS =
                ds.modifyProvenance(_.copy(date = datasetPublishedDates(min = parent1.provenance.date).generateOne))
              updatedDS -> proj.replaceDatasets(updatedDS)
            }
            .generateOne

        val (child2, child2Project) = anyRenkuProjectEntities.importDataset(parent2).generateOne

        val entitiesGrandparent = grandparent.to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]]
        val entitiesParent1 = parent1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
        val entitiesChild1  = child1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
        val entitiesParent2 = parent2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
        val entitiesChild2  = child2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]

        upload(to = projectsDataset, grandparentProject, parent1Project, child1Project, parent2Project, child2Project)

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

        UpdatesCreator
          .prepareUpdatesWhenInvalidated(grandparentProject.resourceId, entitiesGrandparent)
          .runAll(on = projectsDataset)
          .unsafeRunSync()

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
           val (d, p) = anyRenkuProjectEntities
             .addDataset(datasetEntities(provenanceInternal))
             .generateOne
             .leftMap(_.copy(parts = Nil))
           (d, Option.empty[SameAs], p)
         }
        ),
        ("imported external", {
           val (d, p) = anyRenkuProjectEntities
             .addDataset(datasetEntities(provenanceImportedExternal))
             .generateOne
             .leftMap(_.copy(parts = Nil))
           (d, d.provenance.sameAs.some, p)
         }
        )
      )
    } { case (datasetType, (grandparent, maybeGrandparentSameAs, grandparentProject)) =>
      "generate queries for deleted dataset which, " +
        s"in case of $datasetType dataset, " +
        "find datasets which have sameAs pointing to the deleted dataset " +
        "update their sameAs to the deleted dataset sameAs" in {
          val (parent, parentProject) = anyRenkuProjectEntities.importDataset(grandparent)(importedInternal).generateOne
          val (child1, child1Project) = anyRenkuProjectEntities.importDataset(parent)(importedInternal).generateOne
          val (child2, child2Project) = anyRenkuProjectEntities.importDataset(parent)(importedInternal).generateOne

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

          upload(to = projectsDataset, grandparentProject, parentProject, child1Project, child2Project)

          findDatasets.map(onlySameAsAndTop) shouldBe Set(
            (entitiesGrandparent.resourceId.value, maybeGrandparentSameAs.map(_.value), grandparentTopmostSameAs),
            (entitiesParent.resourceId.value, entitiesGrandparent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild1.resourceId.value, entitiesParent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild2.resourceId.value, entitiesParent.resourceId.value.some, grandparentTopmostSameAs)
          )

          UpdatesCreator
            .prepareUpdatesWhenInvalidated(parentProject.resourceId, entitiesParent)
            .runAll(on = projectsDataset)
            .unsafeRunSync()

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
      "updates topmostSameAs on all datasets where topmostSameAs points to the current dataset" +
      "when the topmostSameAs on the current dataset is different than the value in KG" in {
        val theVeryTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value)

        val (ds1, _) = anyRenkuProjectEntities
          .addDataset(
            datasetEntities(provenanceImportedInternal).modify(
              _.modifyProvenance(replaceTopmostSameAs(theVeryTopmostSameAs))
            )
          )
          .generateOne
          .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]], identity)

        val (ds2, ds2Project) = anyRenkuProjectEntities
          .addDataset(
            datasetEntities(provenanceImportedInternal).modify(
              _.modifyProvenance(replaceTopmostSameAs(TopmostSameAs(ds1.resourceId.value)))
            )
          )
          .generateOne
          .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]], identity)

        upload(to = projectsDataset, ds2Project)

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (ds2.resourceId.value, TopmostSameAs(ds1.resourceId.value).value.some)
        )

        UpdatesCreator.prepareUpdates(ds1, Set.empty).runAll(on = projectsDataset).unsafeRunSync()

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (ds2.resourceId.value, theVeryTopmostSameAs.value.some)
        )
      }

    "generate queries which " +
      "updates topmostSameAs for all datasets where topmostSameAs points to the current dataset" +
      "when the topmostSameAs on the current dataset is different than the value in KG -" +
      "case when some datasets have multiple topmostSameAs" in {
        val theVeryTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value)

        val (ds1, _) = anyRenkuProjectEntities
          .addDataset(
            datasetEntities(provenanceImportedInternal).modify(
              _.modifyProvenance(replaceTopmostSameAs(theVeryTopmostSameAs))
            )
          )
          .generateOne
          .leftMap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]])
        val (ds2, ds2Project) = anyRenkuProjectEntities
          .addDataset(
            datasetEntities(provenanceImportedInternal).modify(
              _.modifyProvenance(replaceTopmostSameAs(TopmostSameAs(ds1.resourceId.value)))
            )
          )
          .generateOne
          .leftMap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]])

        upload(to = projectsDataset, ds2Project)

        val otherTopmostSameAs = datasetTopmostSameAs.generateOne
        insert(to = projectsDataset,
               Quad(GraphClass.Project.id(ds2Project.resourceId),
                    ds2.resourceId.asEntityId,
                    renku / "topmostSameAs",
                    otherTopmostSameAs.asEntityId
               )
        )

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (ds2.resourceId.value, TopmostSameAs(ds1.resourceId.value).value.some),
          (ds2.resourceId.value, otherTopmostSameAs.value.some)
        )

        UpdatesCreator.prepareUpdates(ds1, Set.empty).runAll(on = projectsDataset).unsafeRunSync()

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (ds2.resourceId.value, theVeryTopmostSameAs.value.some)
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

      val ds = datasetEntities(provenanceImportedInternal).decoupledFromProject.generateOne
        .to[entities.Dataset[Provenance.ImportedInternal]]

      UpdatesCreator.prepareTopmostSameAsCleanup(projectResourceIds.generateOne,
                                                 ds,
                                                 maybeParentTopmostSameAs = None
      ) shouldBe Nil
    }

    "return updates deleting additional TopmostSameAs if there is parent TopmostSameAs given" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceImportedInternal))
        .generateOne
        .leftMap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]])

      upload(to = projectsDataset, project)

      // simulate DS having two topmostSameAs
      val parentTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value)
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(project.resourceId),
                  ds.resourceId.asEntityId,
                  renku / "topmostSameAs",
                  parentTopmostSameAs.asEntityId
             )
      )

      findDatasets.map(onlyTopmostSameAs) shouldBe Set(
        (ds.resourceId.value, ds.provenance.topmostSameAs.value.some),
        (ds.resourceId.value, parentTopmostSameAs.value.some)
      )

      UpdatesCreator
        .prepareTopmostSameAsCleanup(project.resourceId, ds, maybeParentTopmostSameAs = parentTopmostSameAs.some)
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findDatasets.map(onlyTopmostSameAs) shouldBe Set(
        (ds.resourceId.value, parentTopmostSameAs.value.some)
      )
    }

    "return updates not deleting sole TopmostSameAs if there is parent TopmostSameAs given" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceImportedInternal))
        .generateOne
        .leftMap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]])

      upload(to = projectsDataset, project)

      findDatasets.map(onlyTopmostSameAs) shouldBe Set(
        (ds.resourceId.value, ds.provenance.topmostSameAs.value.some)
      )

      UpdatesCreator
        .prepareTopmostSameAsCleanup(project.resourceId,
                                     ds,
                                     maybeParentTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value).some
        )
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findDatasets.map(onlyTopmostSameAs) shouldBe Set(
        (ds.resourceId.value, ds.provenance.topmostSameAs.value.some)
      )
    }
  }

  "queriesUnlinkingCreators" should {

    "prepare delete queries for all dataset creators existing in KG but not in the model" in {
      forAll(
        anyRenkuProjectEntities
          .addDataset(
            datasetEntities(provenanceNonModified)
              .modify(provenanceLens.modify(creatorsLens.modify(_ => personEntities.generateNonEmptyList())))
          )
          .map(_.map(_.to[entities.RenkuProject]))
      ) { case (ds, project) =>
        val entitiesDS = ds.to[entities.Dataset[entities.Dataset.Provenance]]

        upload(to = projectsDataset, project)

        val creators = ds.provenance.creators
        findCreators(project.resourceId, entitiesDS.resourceId) shouldBe creators.map(_.resourceId).toList.toSet

        val someCreator        = Random.shuffle(creators.toList).head
        val creatorsNotChanged = creators.filterNot(_ == someCreator)
        val newCreators =
          NonEmptyList.fromListUnsafe(creatorsNotChanged ::: personEntities.generateNonEmptyList().toList)
        val dsWithNewCreators = provenanceLens[Dataset.Provenance.NonModified]
          .modify(creatorsLens.modify(_ => newCreators))(ds)
          .to[entities.Dataset[entities.Dataset.Provenance]]

        UpdatesCreator
          .queriesUnlinkingCreators(project.resourceId, dsWithNewCreators, creators.map(_.resourceId).toList.toSet)
          .runAll(on = projectsDataset)
          .unsafeRunSync()

        findCreators(project.resourceId, entitiesDS.resourceId) shouldBe creatorsNotChanged.map(_.resourceId).toSet
      }
    }

    "prepare no queries if there's no change in DS creators" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(
          datasetEntities(provenanceNonModified)
            .modify(provenanceLens.modify(creatorsLens.modify(_ => personEntities.generateNonEmptyList())))
        )
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      UpdatesCreator.queriesUnlinkingCreators(project.resourceId,
                                              ds,
                                              ds.provenance.creators.map(_.resourceId).toList.toSet
      ) shouldBe Nil
    }
  }

  "removeOtherOriginalIdentifiers" should {

    "prepare queries that removes all additional originalIdentifier triples on the DS" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      val existingOriginalId1 = datasetOriginalIdentifiers.generateOne
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(project.resourceId),
                  ds.resourceId.asEntityId,
                  renku / "originalIdentifier",
                  existingOriginalId1.asObject
             )
      )
      val existingOriginalId2 = datasetOriginalIdentifiers.generateOne
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(project.resourceId),
                  ds.resourceId.asEntityId,
                  renku / "originalIdentifier",
                  existingOriginalId2.asObject
             )
      )

      findOriginalIdentifiers(project.resourceId, ds.identification.identifier) shouldBe Set(
        ds.provenance.originalIdentifier,
        existingOriginalId1,
        existingOriginalId2
      )

      UpdatesCreator
        .removeOtherOriginalIdentifiers(project.resourceId, ds, Set(existingOriginalId1, existingOriginalId2))
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findOriginalIdentifiers(project.resourceId, ds.identification.identifier) shouldBe Set(
        ds.provenance.originalIdentifier
      )
    }

    "prepare no queries if there's only the correct originalIdentifier for the DS in KG" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      UpdatesCreator.removeOtherOriginalIdentifiers(project.resourceId,
                                                    ds,
                                                    Set(ds.provenance.originalIdentifier)
      ) shouldBe Nil
    }

    "prepare no queries if there's no originalIdentifier for the DS in KG" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      UpdatesCreator.removeOtherOriginalIdentifiers(project.resourceId, ds, Set.empty) shouldBe Nil
    }
  }

  "removeOtherDSDateCreated" should {

    "prepare queries that removes all additional dateCreated triples on the DS" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      val existingDateCreated1 = datasetCreatedDates(min = ds.provenance.date.instant).generateOne
      insert(
        to = projectsDataset,
        Quad(GraphClass.Project.id(project.resourceId),
             ds.resourceId.asEntityId,
             schema / "dateCreated",
             existingDateCreated1.asObject
        )
      )
      val existingDateCreated2 = datasetCreatedDates(min = ds.provenance.date.instant).generateOne
      insert(
        to = projectsDataset,
        Quad(GraphClass.Project.id(project.resourceId),
             ds.resourceId.asEntityId,
             schema / "dateCreated",
             existingDateCreated2.asObject
        )
      )

      findDateCreated(project.resourceId, ds.identification.identifier) shouldBe Set(ds.provenance.date,
                                                                                     existingDateCreated1,
                                                                                     existingDateCreated2
      )

      UpdatesCreator
        .removeOtherDateCreated(project.resourceId, ds, Set(existingDateCreated1, existingDateCreated2))
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findDateCreated(project.resourceId, ds.identification.identifier) shouldBe Set(ds.provenance.date)
    }

    "prepare no queries if there's only the correct dateCreated for the DS in KG" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]], _.to[entities.Project])

      UpdatesCreator.removeOtherDateCreated(project.resourceId, ds, Set(ds.provenance.date)) shouldBe Nil
    }

    "prepare no queries if it's a DS with datePublished" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceImportedInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]], _.to[entities.Project])

      UpdatesCreator.removeOtherDateCreated(project.resourceId, ds, Set.empty) shouldBe Nil
    }

    "prepare no queries if there's no dateCreated for the DS in KG" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]], _.to[entities.Project])

      UpdatesCreator.removeOtherDateCreated(project.resourceId, ds, Set.empty) shouldBe Nil
    }
  }

  "removeOtherDescriptions" should {

    "prepare queries that removes all additional description triples on the DS" in {
      val description1 = datasetDescriptions.generateOne
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSDesc(description1.some)))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      val description2 = datasetDescriptions.generateOne
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(project.resourceId),
                  ds.resourceId.asEntityId,
                  schema / "description",
                  description2.asObject
             )
      )
      val description3 = datasetDescriptions.generateOne
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(project.resourceId),
                  ds.resourceId.asEntityId,
                  schema / "description",
                  description3.asObject
             )
      )

      findDescriptions(project.resourceId, ds.identification.identifier) shouldBe Set(description1,
                                                                                      description3,
                                                                                      description2
      )

      UpdatesCreator
        .removeOtherDescriptions(project.resourceId, ds, Set(description2, description3))
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findDescriptions(project.resourceId, ds.identification.identifier) shouldBe Set(description1)
    }

    "prepare queries that removes all description triples on the DS if there's no description on DS but some in KG" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSDesc(None)))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      val description = datasetDescriptions.generateOne
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(project.resourceId),
                  ds.resourceId.asEntityId,
                  schema / "description",
                  description.asObject
             )
      )

      findDescriptions(project.resourceId, ds.identification.identifier) shouldBe Set(description)

      UpdatesCreator
        .removeOtherDescriptions(project.resourceId, ds, Set(description))
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findDescriptions(project.resourceId, ds.identification.identifier) shouldBe Set.empty
    }

    "prepare no queries if there's no description on DS and in KG" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSDesc(None)))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      UpdatesCreator.removeOtherDescriptions(project.resourceId, ds, Set.empty) shouldBe Nil
    }

    "prepare no queries if there's only the correct description for the DS in KG" in {
      val description = datasetDescriptions.generateOne
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSDesc(description.some)))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      UpdatesCreator.removeOtherDescriptions(project.resourceId, ds, Set(description)) shouldBe Nil
    }

    "prepare no queries if there's no description for the DS in KG" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSDesc(datasetDescriptions.generateSome)))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      UpdatesCreator.removeOtherDescriptions(project.resourceId, ds, Set.empty) shouldBe Nil
    }
  }

  "removeOtherDSSameAs" should {

    "prepare queries that removes all additional Internal SameAs triples on the DS" in {

      val (originalDS, originalDSProject) =
        anyRenkuProjectEntities.addDataset(datasetEntities(provenanceInternal)).generateOne.map(_.to[entities.Project])
      val (importedDS, importedDSProject) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceImportedInternalAncestorInternal(fixed(SameAs(originalDS.entityId)))))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]],
               _.to[entities.Project]
        )

      upload(to = projectsDataset, originalDSProject, importedDSProject)

      val otherSameAs = datasetSameAs.generateOne.entityId
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(importedDSProject.resourceId),
                  importedDS.resourceId.asEntityId,
                  schema / "sameAs",
                  otherSameAs
             )
      )

      findSameAs(importedDSProject.resourceId, importedDS.identification.identifier)
        .map(_.show) shouldBe Set(importedDS.provenance.sameAs.entityId, otherSameAs).map(_.show)

      UpdatesCreator
        .removeOtherSameAs(importedDSProject.resourceId, importedDS, Set(SameAs(otherSameAs)))
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findSameAs(importedDSProject.resourceId, importedDS.identification.identifier).map(_.show) shouldBe Set(
        importedDS.provenance.sameAs.entityId.show
      )
    }

    "prepare queries that removes all additional External SameAs triples on the DS" in {

      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceImportedExternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      val otherSameAs = datasetSameAs.generateOne.entityId
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(project.resourceId), ds.resourceId.asEntityId, schema / "sameAs", otherSameAs)
      )

      findSameAs(project.resourceId, ds.identification.identifier).map(_.show) shouldBe
        Set(ds.provenance.sameAs.entityId, otherSameAs).map(_.show)

      UpdatesCreator
        .removeOtherSameAs(project.resourceId, ds, Set(SameAs(otherSameAs)))
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findSameAs(project.resourceId, ds.identification.identifier).map(_.show) shouldBe
        Set(ds.provenance.sameAs.entityId.show)
    }

    "prepare no queries if there's only the correct sameAs for the DS in KG" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceImportedInternalAncestorInternal(datasetInternalSameAs.generateOne)))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]],
               _.to[entities.Project]
        )

      UpdatesCreator.removeOtherSameAs(project.resourceId, ds, Set(SameAs(ds.provenance.sameAs.entityId))) shouldBe Nil
    }

    "prepare no queries if there's no sameAs for the DS in KG" in {
      val (ds, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceImportedInternalAncestorInternal(datasetInternalSameAs.generateOne)))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]],
               _.to[entities.Project]
        )

      UpdatesCreator.removeOtherSameAs(project.resourceId, ds, Set.empty) shouldBe Nil
    }
  }

  "deleteOtherDerivedFrom" should {

    "prepare queries that removes other wasDerivedFrom from the given DS" in {
      val (_ -> modification, project) = anyRenkuProjectEntities
        .addDatasetAndModification(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.bimap(identity, _.to[entities.Dataset[entities.Dataset.Provenance.Modified]]), _.to[entities.Project])

      upload(to = projectsDataset, project)

      val otherDerivedFrom       = datasetDerivedFroms.generateOne
      val otherDerivedFromJsonLD = otherDerivedFrom.asJsonLD
      upload(to = projectsDataset,
             NamedGraph.fromJsonLDsUnsafe(GraphClass.Project.id(project.resourceId), otherDerivedFromJsonLD)
      )

      val derivedFromId = otherDerivedFromJsonLD.entityId.getOrElse(fail(" Cannot obtain EntityId for DerivedFrom"))
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(project.resourceId),
                  modification.resourceId.asEntityId,
                  prov / "wasDerivedFrom",
                  derivedFromId
             )
      )

      findDerivedFrom(project.resourceId, modification.identification.identifier) shouldBe Set(
        modification.provenance.derivedFrom,
        otherDerivedFrom
      )

      UpdatesCreator
        .deleteOtherDerivedFrom(project.resourceId, modification)
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findDerivedFrom(project.resourceId, modification.identification.identifier) shouldBe Set(
        modification.provenance.derivedFrom
      )
    }
  }

  "deleteOtherTopmostDerivedFrom" should {

    "prepare queries that removes other topmostDerivedFrom from the given DS" in {
      val (_ -> modification, project) = anyRenkuProjectEntities
        .addDatasetAndModification(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.bimap(identity, _.to[entities.Dataset[entities.Dataset.Provenance.Modified]]), _.to[entities.Project])

      upload(to = projectsDataset, project)

      val otherTopmostDerivedFrom = datasetTopmostDerivedFroms.generateOne
      insert(
        to = projectsDataset,
        Quad(GraphClass.Project.id(project.resourceId),
             modification.resourceId.asEntityId,
             renku / "topmostDerivedFrom",
             otherTopmostDerivedFrom.asEntityId
        )
      )

      findTopmostDerivedFrom(project.resourceId, modification.identification.identifier) shouldBe Set(
        modification.provenance.topmostDerivedFrom,
        otherTopmostDerivedFrom
      )

      UpdatesCreator
        .deleteOtherTopmostDerivedFrom(project.resourceId, modification)
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findTopmostDerivedFrom(project.resourceId, modification.identification.identifier) shouldBe
        Set(modification.provenance.topmostDerivedFrom)
    }
  }

  "deletePublicationEvents" should {

    "prepare deletion queries that removes publication events linked to the given DS" in {

      val project = anyRenkuProjectEntities
        .withDatasets(datasetEntities(provenanceNonModified), datasetEntities(provenanceNonModified))
        .generateOne
        .to[entities.Project]

      upload(to = projectsDataset, project)

      val dsToClean :: dsToStay :: Nil = project.datasets

      findPublicationEvents(project.resourceId, dsToClean.resourceId) shouldBe dsToClean.publicationEvents.toSet
      findPublicationEvents(project.resourceId, dsToStay.resourceId)  shouldBe dsToStay.publicationEvents.toSet

      UpdatesCreator
        .deletePublicationEvents(project.resourceId, dsToClean)
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findPublicationEvents(project.resourceId, dsToClean.resourceId) shouldBe Set.empty
      findPublicationEvents(project.resourceId, dsToStay.resourceId)  shouldBe dsToStay.publicationEvents.toSet
    }
  }

  private def findDatasets: Set[(String, Option[String], Option[String], Option[String], Option[String])] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "fetch ds data",
        Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
        s"""|SELECT ?id ?maybeSameAs ?maybeTopmostSameAs ?maybeDerivedFrom ?maybeTopmostDerivedFrom
            |WHERE {
            |  GRAPH ?g {
            |    ?id a schema:Dataset .
            |    OPTIONAL { ?id schema:sameAs/schema:url ?maybeSameAs } .
            |    OPTIONAL { ?id renku:topmostSameAs ?maybeTopmostSameAs } .
            |    OPTIONAL { ?id prov:wasDerivedFrom/schema:url ?maybeDerivedFrom } .
            |    OPTIONAL { ?id renku:topmostDerivedFrom ?maybeTopmostDerivedFrom } .
            |  }
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

  private def findCreators(projectId: projects.ResourceId, resourceId: datasets.ResourceId): Set[persons.ResourceId] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "fetch ds creator",
        Prefixes of schema -> "schema",
        s"""|SELECT ?personId
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                      schema:creator ?personId
            |  }
            |}
            |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => persons.ResourceId.from(row("personId")))
      .sequence
      .fold(throw _, identity)
      .toSet

  private def findOriginalIdentifiers(projectId: projects.ResourceId,
                                      id:        datasets.Identifier
  ): Set[datasets.OriginalIdentifier] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "fetch ds originalIdentifier",
        Prefixes.of(renku -> "renku", schema -> "schema"),
        s"""|SELECT ?originalId
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ?id a schema:Dataset;
            |        schema:identifier '$id';
            |        renku:originalIdentifier ?originalId.
            |  }
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.OriginalIdentifier(row("originalId")))
      .toSet

  private def findDateCreated(projectId: projects.ResourceId, id: datasets.Identifier): Set[datasets.DateCreated] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "fetch ds date",
        Prefixes of schema -> "schema",
        s"""|SELECT ?date
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ?id a schema:Dataset;
            |        schema:identifier '$id';
            |        schema:dateCreated ?date.
            |  }
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.DateCreated(Instant.parse(row("date"))))
      .toSet

  private def findDescriptions(projectId: projects.ResourceId, id: datasets.Identifier): Set[datasets.Description] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "fetch ds description",
        Prefixes of schema -> "schema",
        s"""|SELECT ?desc
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ?id a schema:Dataset;
            |        schema:identifier '$id';
            |        schema:description ?desc.
            |  }
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.Description(row("desc")))
      .toSet

  private def findSameAs(projectId: projects.ResourceId, id: datasets.Identifier): Set[datasets.SameAs] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "fetch sameAs",
        Prefixes of schema -> "schema",
        s"""|SELECT ?sameAs
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ?id a schema:Dataset;
            |        schema:identifier '$id';
            |        schema:sameAs ?sameAs.
            |  }
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.SameAs(row("sameAs")))
      .toSet

  private def findTopmostDerivedFrom(projectId: projects.ResourceId,
                                     id:        datasets.Identifier
  ): Set[datasets.TopmostDerivedFrom] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "fetch topmostDerivedFrom",
        Prefixes.of(renku -> "renku", schema -> "schema"),
        s"""|SELECT ?topmostDerivedFrom
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ?id a schema:Dataset;
            |        schema:identifier '$id';
            |        renku:topmostDerivedFrom ?topmostDerivedFrom
            |   }
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.TopmostDerivedFrom(row("topmostDerivedFrom")))
      .toSet

  private def findDerivedFrom(projectId: projects.ResourceId, id: datasets.Identifier): Set[datasets.DerivedFrom] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "fetch derivedFrom",
        Prefixes.of(prov -> "prov", schema -> "schema"),
        s"""|SELECT ?derivedFrom
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ?id a schema:Dataset;
            |        schema:identifier '$id';
            |        prov:wasDerivedFrom/schema:url ?derivedFrom
            |  }
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => datasets.DerivedFrom(row("derivedFrom")))
      .toSet

  private def findPublicationEvents(projectId: projects.ResourceId,
                                    dsId:      datasets.ResourceId
  ): Set[entities.PublicationEvent] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "fetch PublicationEvents",
        Prefixes of schema -> "schema",
        sparql"""|SELECT ?id ?about ?dsId ?maybeDesc ?name ?date
                 |WHERE {
                 |  GRAPH ${GraphClass.Project.id(projectId)} {
                 |    BIND (${dsId.asEntityId} AS ?dsId)
                 |    ?about schema:url ?dsId.
                 |    ?id a schema:PublicationEvent;
                 |          schema:about ?about;
                 |          schema:name ?name;
                 |          schema:startDate ?date.
                 |    OPTIONAL { ?id schema:description ?maybeDesc }
                 |  }
                 |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row =>
        entities.PublicationEvent(
          publicationEvents.ResourceId(row("id")),
          publicationEvents.About(row("about")),
          datasets.ResourceId(row("dsId")),
          row.get("maybeDesc").map(publicationEvents.Description(_)),
          publicationEvents.Name(row("name")),
          publicationEvents.StartDate(Instant.parse(row("date")))
        )
      )
      .toSet

  private implicit class SameAsOps(sameAs: SameAs) {
    lazy val entityId: EntityId = sameAs.asJsonLD.entityId.getOrElse(fail("Cannot obtain sameAs @id"))
  }
}
