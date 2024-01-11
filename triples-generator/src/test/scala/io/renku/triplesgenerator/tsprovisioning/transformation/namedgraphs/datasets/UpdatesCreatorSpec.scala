/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{countingGen, fixed}
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model._
import io.renku.graph.model.datasets.{SameAs, TopmostSameAs}
import io.renku.graph.model.entities.Dataset.Provenance
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.testentities.{Dataset, ModelOps}
import io.renku.graph.model.views.RdfResource
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, NamedGraph}
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.util.Random

class UpdatesCreatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with should.Matchers
    with EntitiesGenerators
    with ModelOps
    with ScalaCheckPropertyChecks {

  "prepareUpdatesWhenInvalidated" should {

    "generate queries for deleted dataset which, " +
      "in case of internal dataset, " +
      "find datasets which have sameAs pointing to the deleted dataset " +
      "update their sameAs to None " +
      "select dataset with the oldest date " +
      "and update all datasets which have topmostSameAs pointing to the deleted DS with the selected resourceId" in projectsDSConfig
        .use { implicit pcc =>
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
          val entitiesParent1 =
            parent1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
          val entitiesParent2 =
            parent2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
          val entitiesChild2 = child2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]

          for {
            _ <- uploadToProjects(grandparentProject, parent1Project, parent2Project, child2Project)

            _ <- findDatasets.asserting {
                   _.map(onlySameAsAndTop) shouldBe Set(
                     (entitiesGrandparent.resourceId.value,
                      None,
                      entitiesGrandparent.provenance.topmostSameAs.value.some
                     ),
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
                 }

            _ <- runUpdates(UpdatesCreator.prepareUpdatesWhenInvalidated(entitiesGrandparent))

            _ <- findDatasets.asserting {
                   _.map(onlySameAsAndTop) shouldBe Set(
                     (entitiesGrandparent.resourceId.value,
                      None,
                      entitiesGrandparent.provenance.topmostSameAs.value.some
                     ),
                     (entitiesParent1.resourceId.value, None, entitiesParent1.resourceId.value.some),
                     (entitiesParent2.resourceId.value, None, entitiesParent1.resourceId.value.some),
                     (entitiesChild2.resourceId.value,
                      entitiesParent2.resourceId.value.some,
                      entitiesParent1.resourceId.value.some
                     )
                   )
                 }
          } yield Succeeded
        }

    "generate queries for deleted dataset which, " +
      "in case of imported external dataset, " +
      "find datasets which have sameAs pointing to the deleted dataset " +
      "update their sameAs to their topmostSameAs" in projectsDSConfig.use { implicit pcc =>
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

        for {
          _ <- uploadToProjects(grandparentProject, parent1Project, child1Project, parent2Project, child2Project)

          _ <- findDatasets.asserting {
                 _.map(onlySameAsAndTop) shouldBe Set(
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
               }

          _ <- runUpdates {
                 UpdatesCreator.prepareUpdatesWhenInvalidated(grandparentProject.resourceId, entitiesGrandparent)
               }

          _ <- findDatasets.asserting {
                 _.map(onlySameAsAndTop) shouldBe Set(
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
        } yield Succeeded
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
        "update their sameAs to the deleted dataset sameAs" in projectsDSConfig.use { implicit pcc =>
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

          for {
            _ <- uploadToProjects(grandparentProject, parentProject, child1Project, child2Project)

            _ <- findDatasets.asserting {
                   _.map(onlySameAsAndTop) shouldBe Set(
                     (entitiesGrandparent.resourceId.value,
                      maybeGrandparentSameAs.map(_.value),
                      grandparentTopmostSameAs
                     ),
                     (entitiesParent.resourceId.value,
                      entitiesGrandparent.resourceId.value.some,
                      grandparentTopmostSameAs
                     ),
                     (entitiesChild1.resourceId.value, entitiesParent.resourceId.value.some, grandparentTopmostSameAs),
                     (entitiesChild2.resourceId.value, entitiesParent.resourceId.value.some, grandparentTopmostSameAs)
                   )
                 }

            _ <- runUpdates(UpdatesCreator.prepareUpdatesWhenInvalidated(parentProject.resourceId, entitiesParent))

            _ <- findDatasets.asserting {
                   _.map(onlySameAsAndTop) shouldBe Set(
                     (entitiesGrandparent.resourceId.value,
                      maybeGrandparentSameAs.map(_.value),
                      grandparentTopmostSameAs
                     ),
                     (entitiesParent.resourceId.value,
                      entitiesGrandparent.resourceId.value.some,
                      grandparentTopmostSameAs
                     ),
                     (entitiesChild1.resourceId.value,
                      entitiesGrandparent.resourceId.value.some,
                      grandparentTopmostSameAs
                     ),
                     (entitiesChild2.resourceId.value,
                      entitiesGrandparent.resourceId.value.some,
                      grandparentTopmostSameAs
                     )
                   )
                 }
          } yield Succeeded
        }
    }
  }

  "prepareUpdates" should {

    "generate queries which " +
      "updates topmostSameAs on all datasets where topmostSameAs points to the current dataset" +
      "when the topmostSameAs on the current dataset is different than the value in KG" in projectsDSConfig.use {
        implicit pcc =>
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

          for {
            _ <- uploadToProjects(ds2Project)

            _ <- findDatasets.asserting {
                   _.map(onlyTopmostSameAs) shouldBe Set(
                     (ds2.resourceId.value, TopmostSameAs(ds1.resourceId.value).value.some)
                   )
                 }

            _ <- runUpdates(UpdatesCreator.prepareUpdates(ds1, Set.empty))

            _ <- findDatasets.asserting {
                   _.map(onlyTopmostSameAs) shouldBe Set(
                     (ds2.resourceId.value, theVeryTopmostSameAs.value.some)
                   )
                 }
          } yield Succeeded
      }

    "generate queries which " +
      "updates topmostSameAs for all datasets where topmostSameAs points to the current dataset" +
      "when the topmostSameAs on the current dataset is different than the value in KG -" +
      "case when some datasets have multiple topmostSameAs" in projectsDSConfig.use { implicit pcc =>
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

        for {
          _ <- uploadToProjects(ds2Project)

          otherTopmostSameAs = datasetTopmostSameAs.generateOne
          _ <- insert(
                 Quad(GraphClass.Project.id(ds2Project.resourceId),
                      ds2.resourceId.asEntityId,
                      renku / "topmostSameAs",
                      otherTopmostSameAs.asEntityId
                 )
               )

          _ <- findDatasets.asserting {
                 _.map(onlyTopmostSameAs) shouldBe Set(
                   (ds2.resourceId.value, TopmostSameAs(ds1.resourceId.value).value.some),
                   (ds2.resourceId.value, otherTopmostSameAs.value.some)
                 )
               }

          _ <- runUpdates(UpdatesCreator.prepareUpdates(ds1, Set.empty))

          _ <- findDatasets.asserting {
                 _.map(onlyTopmostSameAs) shouldBe Set((ds2.resourceId.value, theVeryTopmostSameAs.value.some))
               }
        } yield Succeeded
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

    "return updates deleting additional TopmostSameAs if there is parent TopmostSameAs given" in projectsDSConfig.use {
      implicit pcc =>
        val (ds, project) = anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceImportedInternal))
          .generateOne
          .leftMap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]])

        for {
          _ <- uploadToProjects(project)

          // simulate DS having two topmostSameAs
          parentTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value)
          _ <- insert(
                 Quad(GraphClass.Project.id(project.resourceId),
                      ds.resourceId.asEntityId,
                      renku / "topmostSameAs",
                      parentTopmostSameAs.asEntityId
                 )
               )

          _ <- findDatasets.asserting {
                 _.map(onlyTopmostSameAs) shouldBe Set(
                   (ds.resourceId.value, ds.provenance.topmostSameAs.value.some),
                   (ds.resourceId.value, parentTopmostSameAs.value.some)
                 )
               }

          _ <- runUpdates {
                 UpdatesCreator.prepareTopmostSameAsCleanup(project.resourceId,
                                                            ds,
                                                            maybeParentTopmostSameAs = parentTopmostSameAs.some
                 )
               }

          _ <- findDatasets.asserting {
                 _.map(onlyTopmostSameAs) shouldBe Set((ds.resourceId.value, parentTopmostSameAs.value.some))
               }
        } yield Succeeded
    }

    "return updates not deleting sole TopmostSameAs if there is parent TopmostSameAs given" in projectsDSConfig.use {
      implicit pcc =>
        val (ds, project) = anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceImportedInternal))
          .generateOne
          .leftMap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]])

        for {
          _ <- uploadToProjects(project)

          _ <- findDatasets.asserting {
                 _.map(onlyTopmostSameAs) shouldBe Set((ds.resourceId.value, ds.provenance.topmostSameAs.value.some))
               }

          _ <- runUpdates {
                 UpdatesCreator.prepareTopmostSameAsCleanup(project.resourceId,
                                                            ds,
                                                            maybeParentTopmostSameAs =
                                                              TopmostSameAs(datasetResourceIds.generateOne.value).some
                 )
               }

          _ <- findDatasets.asserting {
                 _.map(onlyTopmostSameAs) shouldBe Set((ds.resourceId.value, ds.provenance.topmostSameAs.value.some))
               }
        } yield Succeeded
    }
  }

  "queriesUnlinkingCreators" should {

    forAll(
      anyRenkuProjectEntities
        .addDataset(
          datasetEntities(provenanceNonModified)
            .modify(provenanceLens.modify(creatorsLens.modify(_ => personEntities.generateNonEmptyList())))
        )
        .map(_.map(_.to[entities.RenkuProject])),
      countingGen
    ) { case ((ds, project), attempt) =>
      s"prepare delete queries for all dataset creators existing in KG but not in the model - #$attempt" in projectsDSConfig
        .use { implicit pcc =>
          val entitiesDS = ds.to[entities.Dataset[entities.Dataset.Provenance]]

          for {
            _ <- uploadToProjects(project)

            creators = ds.provenance.creators
            _ <- findCreators(project.resourceId, entitiesDS.resourceId)
                   .asserting(_ shouldBe creators.map(_.resourceId).toList.toSet)

            someCreator        = Random.shuffle(creators.toList).head
            creatorsNotChanged = creators.filterNot(_ == someCreator)
            newCreators =
              NonEmptyList.fromListUnsafe(creatorsNotChanged ::: personEntities.generateNonEmptyList().toList)
            dsWithNewCreators = provenanceLens[Dataset.Provenance.NonModified]
                                  .modify(creatorsLens.modify(_ => newCreators))(ds)
                                  .to[entities.Dataset[entities.Dataset.Provenance]]

            _ <- runUpdates {
                   UpdatesCreator.queriesUnlinkingCreators(project.resourceId,
                                                           dsWithNewCreators,
                                                           creators.map(_.resourceId).toList.toSet
                   )
                 }

            _ <- findCreators(project.resourceId, entitiesDS.resourceId)
                   .asserting(_ shouldBe creatorsNotChanged.map(_.resourceId).toSet)
          } yield Succeeded
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

    "prepare queries that removes all additional originalIdentifier triples on the DS" in projectsDSConfig.use {
      implicit pcc =>
        val (ds, project) = anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceNonModified))
          .generateOne
          .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

        for {
          _ <- uploadToProjects(project)

          existingOriginalId1 = datasetOriginalIdentifiers.generateOne
          _ <- insert(
                 Quad(GraphClass.Project.id(project.resourceId),
                      ds.resourceId.asEntityId,
                      renku / "originalIdentifier",
                      existingOriginalId1.asObject
                 )
               )
          existingOriginalId2 = datasetOriginalIdentifiers.generateOne
          _ <- insert(
                 Quad(GraphClass.Project.id(project.resourceId),
                      ds.resourceId.asEntityId,
                      renku / "originalIdentifier",
                      existingOriginalId2.asObject
                 )
               )

          _ <- findOriginalIdentifiers(project.resourceId, ds.identification.identifier)
                 .asserting {
                   _ shouldBe Set(ds.provenance.originalIdentifier, existingOriginalId1, existingOriginalId2)
                 }

          _ <- runUpdates {
                 UpdatesCreator.removeOtherOriginalIdentifiers(project.resourceId,
                                                               ds,
                                                               Set(existingOriginalId1, existingOriginalId2)
                 )
               }

          _ <- findOriginalIdentifiers(project.resourceId, ds.identification.identifier)
                 .asserting(_ shouldBe Set(ds.provenance.originalIdentifier))
        } yield Succeeded
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

    "prepare queries that removes all additional dateCreated triples on the DS" in projectsDSConfig.use {
      implicit pcc =>
        val (ds, project) = anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceInternal))
          .generateOne
          .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]], _.to[entities.Project])

        for {
          _ <- uploadToProjects(project)

          existingDateCreated1 = datasetCreatedDates(min = ds.provenance.date.instant).generateOne
          _ <- insert(
                 Quad(GraphClass.Project.id(project.resourceId),
                      ds.resourceId.asEntityId,
                      schema / "dateCreated",
                      existingDateCreated1.asObject
                 )
               )
          existingDateCreated2 = datasetCreatedDates(min = ds.provenance.date.instant).generateOne
          _ <- insert(
                 Quad(GraphClass.Project.id(project.resourceId),
                      ds.resourceId.asEntityId,
                      schema / "dateCreated",
                      existingDateCreated2.asObject
                 )
               )

          _ <- findDateCreated(project.resourceId, ds.identification.identifier)
                 .asserting(_ shouldBe Set(ds.provenance.date, existingDateCreated1, existingDateCreated2))

          _ <- runUpdates {
                 UpdatesCreator.removeOtherDateCreated(project.resourceId,
                                                       ds,
                                                       Set(existingDateCreated1, existingDateCreated2)
                 )
               }

          _ <- findDateCreated(project.resourceId, ds.identification.identifier)
                 .asserting(_ shouldBe Set(ds.provenance.date))
        } yield Succeeded
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

    "prepare queries that removes all additional description triples on the DS" in projectsDSConfig.use {
      implicit pcc =>
        val description1 = datasetDescriptions.generateOne
        val (ds, project) = anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSDesc(description1.some)))
          .generateOne
          .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

        for {
          _ <- uploadToProjects(project)

          description2 = datasetDescriptions.generateOne
          _ <- insert(
                 Quad(GraphClass.Project.id(project.resourceId),
                      ds.resourceId.asEntityId,
                      schema / "description",
                      description2.asObject
                 )
               )
          description3 = datasetDescriptions.generateOne
          _ <- insert(
                 Quad(GraphClass.Project.id(project.resourceId),
                      ds.resourceId.asEntityId,
                      schema / "description",
                      description3.asObject
                 )
               )

          _ <- findDescriptions(project.resourceId, ds.identification.identifier)
                 .asserting(_ shouldBe Set(description1, description3, description2))

          _ <- runUpdates {
                 UpdatesCreator.removeOtherDescriptions(project.resourceId, ds, Set(description2, description3))
               }

          _ <- findDescriptions(project.resourceId, ds.identification.identifier)
                 .asserting(_ shouldBe Set(description1))
        } yield Succeeded
    }

    "prepare queries that removes all description triples on the DS if there's no description on DS but some in KG" in projectsDSConfig
      .use { implicit pcc =>
        val (ds, project) = anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSDesc(None)))
          .generateOne
          .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

        for {
          _ <- uploadToProjects(project)

          description = datasetDescriptions.generateOne
          _ <- insert(
                 Quad(GraphClass.Project.id(project.resourceId),
                      ds.resourceId.asEntityId,
                      schema / "description",
                      description.asObject
                 )
               )

          _ <- findDescriptions(project.resourceId, ds.identification.identifier)
                 .asserting(_ shouldBe Set(description))

          _ <- runUpdates {
                 UpdatesCreator.removeOtherDescriptions(project.resourceId, ds, Set(description))
               }

          _ <- findDescriptions(project.resourceId, ds.identification.identifier)
                 .asserting(_ shouldBe Set.empty)
        } yield Succeeded
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

    "prepare queries that removes all additional Internal SameAs triples on the DS" in projectsDSConfig.use {
      implicit pcc =>
        val (originalDS, originalDSProject) =
          anyRenkuProjectEntities
            .addDataset(datasetEntities(provenanceInternal))
            .generateOne
            .map(_.to[entities.Project])
        val (importedDS, importedDSProject) = anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceImportedInternalAncestorInternal(fixed(SameAs(originalDS.entityId)))))
          .generateOne
          .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]],
                 _.to[entities.Project]
          )

        for {
          _ <- uploadToProjects(originalDSProject, importedDSProject)

          otherSameAs = datasetSameAs.generateOne.entityId
          _ <- insert(
                 Quad(GraphClass.Project.id(importedDSProject.resourceId),
                      importedDS.resourceId.asEntityId,
                      schema / "sameAs",
                      otherSameAs
                 )
               )

          _ <- findSameAs(importedDSProject.resourceId, importedDS.identification.identifier).asserting {
                 _.map(_.show) shouldBe Set(importedDS.provenance.sameAs.entityId, otherSameAs).map(_.show)
               }

          _ <- runUpdates {
                 UpdatesCreator.removeOtherSameAs(importedDSProject.resourceId, importedDS, Set(SameAs(otherSameAs)))
               }

          _ <- findSameAs(importedDSProject.resourceId, importedDS.identification.identifier)
                 .asserting(_.map(_.show) shouldBe Set(importedDS.provenance.sameAs.entityId.show))
        } yield Succeeded
    }

    "prepare queries that removes all additional External SameAs triples on the DS" in projectsDSConfig.use {
      implicit pcc =>
        val (ds, project) = anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceImportedExternal))
          .generateOne
          .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]], _.to[entities.Project])

        for {
          _ <- uploadToProjects(project)

          otherSameAs = datasetSameAs.generateOne.entityId
          _ <-
            insert(
              Quad(GraphClass.Project.id(project.resourceId), ds.resourceId.asEntityId, schema / "sameAs", otherSameAs)
            )

          _ <- findSameAs(project.resourceId, ds.identification.identifier)
                 .asserting(_.map(_.show) shouldBe Set(ds.provenance.sameAs.entityId, otherSameAs).map(_.show))

          _ <- runUpdates {
                 UpdatesCreator.removeOtherSameAs(project.resourceId, ds, Set(SameAs(otherSameAs)))
               }

          _ <- findSameAs(project.resourceId, ds.identification.identifier)
                 .asserting(_.map(_.show) shouldBe Set(ds.provenance.sameAs.entityId.show))
        } yield Succeeded
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

    "prepare queries that removes other wasDerivedFrom from the given DS" in projectsDSConfig.use { implicit pcc =>
      val (_ -> modification, project) = anyRenkuProjectEntities
        .addDatasetAndModification(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.bimap(identity, _.to[entities.Dataset[entities.Dataset.Provenance.Modified]]), _.to[entities.Project])

      for {
        _ <- uploadToProjects(project)

        otherDerivedFrom       = datasetDerivedFroms.generateOne
        otherDerivedFromJsonLD = otherDerivedFrom.asJsonLD
        _ <- uploadToProjects(
               NamedGraph.fromJsonLDsUnsafe(GraphClass.Project.id(project.resourceId), otherDerivedFromJsonLD)
             )

        derivedFromId = otherDerivedFromJsonLD.entityId.getOrElse(fail(" Cannot obtain EntityId for DerivedFrom"))
        _ <- insert(
               Quad(GraphClass.Project.id(project.resourceId),
                    modification.resourceId.asEntityId,
                    prov / "wasDerivedFrom",
                    derivedFromId
               )
             )

        _ <- findDerivedFrom(project.resourceId, modification.identification.identifier)
               .asserting(_ shouldBe Set(modification.provenance.derivedFrom, otherDerivedFrom))

        _ <- runUpdates {
               UpdatesCreator.deleteOtherDerivedFrom(project.resourceId, modification)
             }

        _ <- findDerivedFrom(project.resourceId, modification.identification.identifier)
               .asserting(_ shouldBe Set(modification.provenance.derivedFrom))
      } yield Succeeded
    }
  }

  "deleteOtherTopmostDerivedFrom" should {

    "prepare queries that removes other topmostDerivedFrom from the given DS" in projectsDSConfig.use { implicit pcc =>
      val (_ -> modification, project) = anyRenkuProjectEntities
        .addDatasetAndModification(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.bimap(identity, _.to[entities.Dataset[entities.Dataset.Provenance.Modified]]), _.to[entities.Project])

      for {
        _ <- uploadToProjects(project)

        otherTopmostDerivedFrom = datasetTopmostDerivedFroms.generateOne
        _ <- insert(
               Quad(GraphClass.Project.id(project.resourceId),
                    modification.resourceId.asEntityId,
                    renku / "topmostDerivedFrom",
                    otherTopmostDerivedFrom.asEntityId
               )
             )

        _ <- findTopmostDerivedFrom(project.resourceId, modification.identification.identifier)
               .asserting(_ shouldBe Set(modification.provenance.topmostDerivedFrom, otherTopmostDerivedFrom))

        _ <- runUpdates {
               UpdatesCreator.deleteOtherTopmostDerivedFrom(project.resourceId, modification)
             }

        _ <- findTopmostDerivedFrom(project.resourceId, modification.identification.identifier)
               .asserting(_ shouldBe Set(modification.provenance.topmostDerivedFrom))
      } yield Succeeded
    }
  }

  "deletePublicationEvents" should {

    "prepare deletion queries that removes publication events linked to the given DS" in projectsDSConfig.use {
      implicit pcc =>
        val project = anyRenkuProjectEntities
          .withDatasets(datasetEntities(provenanceNonModified), datasetEntities(provenanceNonModified))
          .generateOne
          .to[entities.Project]

        for {
          _ <- uploadToProjects(project)

          dsToClean :: dsToStay :: Nil = project.datasets

          _ <- findPublicationEvents(project.resourceId, dsToClean.resourceId)
                 .asserting(_ shouldBe dsToClean.publicationEvents.toSet)
          _ <- findPublicationEvents(project.resourceId, dsToStay.resourceId)
                 .asserting(_ shouldBe dsToStay.publicationEvents.toSet)

          _ <- runUpdates {
                 UpdatesCreator.deletePublicationEvents(project.resourceId, dsToClean)
               }

          _ <- findPublicationEvents(project.resourceId, dsToClean.resourceId)
                 .asserting(_ shouldBe Set.empty)
          _ <- findPublicationEvents(project.resourceId, dsToStay.resourceId)
                 .asserting(_ shouldBe dsToStay.publicationEvents.toSet)
        } yield Succeeded
    }
  }

  private def findDatasets(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Set[(String, Option[String], Option[String], Option[String], Option[String])]] =
    runSelect(
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
    ).map(
      _.map(row =>
        (row("id"),
         row.get("maybeSameAs"),
         row.get("maybeTopmostSameAs"),
         row.get("maybeDerivedFrom"),
         row.get("maybeTopmostDerivedFrom")
        )
      ).toSet
    )

  private lazy val onlyTopmostSameAs
      : ((String, Option[String], Option[String], Option[String], Option[String])) => (String, Option[String]) = {
    case (resourceId, _, maybeTopmostSameAs, _, _) => resourceId -> maybeTopmostSameAs
  }

  private lazy val onlySameAsAndTop: (
      (String, Option[String], Option[String], Option[String], Option[String])
  ) => (String, Option[String], Option[String]) = { case (resourceId, sameAs, maybeTopmostSameAs, _, _) =>
    (resourceId, sameAs, maybeTopmostSameAs)
  }

  private def findCreators(projectId: projects.ResourceId, resourceId: datasets.ResourceId)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Set[persons.ResourceId]] =
    runSelect(
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
    ).map(
      _.map(row => persons.ResourceId.from(row("personId"))).sequence
        .fold(throw _, identity)
        .toSet
    )

  private def findOriginalIdentifiers(projectId: projects.ResourceId, id: datasets.Identifier)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Set[datasets.OriginalIdentifier]] =
    runSelect(
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
    ).map(_.map(row => datasets.OriginalIdentifier(row("originalId"))).toSet)

  private def findDateCreated(projectId: projects.ResourceId, id: datasets.Identifier)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Set[datasets.DateCreated]] =
    runSelect(
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
    ).map(_.map(row => datasets.DateCreated(Instant.parse(row("date")))).toSet)

  private def findDescriptions(projectId: projects.ResourceId, id: datasets.Identifier)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Set[datasets.Description]] =
    runSelect(
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
    ).map(_.map(row => datasets.Description(row("desc"))).toSet)

  private def findSameAs(projectId: projects.ResourceId, id: datasets.Identifier)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Set[datasets.SameAs]] =
    runSelect(
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
    ).map(_.map(row => datasets.SameAs(row("sameAs"))).toSet)

  private def findTopmostDerivedFrom(projectId: projects.ResourceId, id: datasets.Identifier)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Set[datasets.TopmostDerivedFrom]] =
    runSelect(
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
    ).map(_.map(row => datasets.TopmostDerivedFrom(row("topmostDerivedFrom"))).toSet)

  private def findDerivedFrom(projectId: projects.ResourceId, id: datasets.Identifier)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Set[datasets.DerivedFrom]] =
    runSelect(
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
    ).map(_.map(row => datasets.DerivedFrom(row("derivedFrom"))).toSet)

  private def findPublicationEvents(projectId: projects.ResourceId, dsId: datasets.ResourceId)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Set[entities.PublicationEvent]] =
    runSelect(
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
    ).map(
      _.map(row =>
        entities.PublicationEvent(
          publicationEvents.ResourceId(row("id")),
          publicationEvents.About(row("about")),
          datasets.ResourceId(row("dsId")),
          row.get("maybeDesc").map(publicationEvents.Description(_)),
          publicationEvents.Name(row("name")),
          publicationEvents.StartDate(Instant.parse(row("date")))
        )
      ).toSet
    )

  private implicit class SameAsOps(sameAs: SameAs) {
    lazy val entityId: EntityId = sameAs.asJsonLD.entityId.getOrElse(fail("Cannot obtain sameAs @id"))
  }

  implicit lazy val ioLogger: Logger[IO] = TestLogger()
}
