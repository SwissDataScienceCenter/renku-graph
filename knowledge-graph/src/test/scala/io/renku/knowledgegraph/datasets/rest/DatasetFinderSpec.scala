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

package io.renku.knowledgegraph.datasets.rest

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.datasets.SameAs
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.datasets.model._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.{InMemoryJenaForSpec, RenkuDataset, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class DatasetFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with RenkuDataset
    with ScalaCheckPropertyChecks
    with IOSpec {

  "findDataset" should {

    "return details of the dataset with the given id " +
      "- a case of a non-modified renku dataset used in a single project" in new TestCase {

        forAll(
          anyRenkuProjectEntities(visibilityPublic) addDataset datasetEntities(provenanceInternal),
          anyRenkuProjectEntities(visibilityPublic) addDataset datasetEntities(provenanceNonModified)
        ) { case ((dataset, project), (_, otherProject)) =>
          upload(to = renkuDataset, project, otherProject)

          datasetFinder
            .findDataset(dataset.identifier, AuthContext(None, dataset.identifier, Set(project.path)))
            .unsafeRunSync() shouldBe internalToNonModified(dataset, project).some
        }
      }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are using the same imported dataset" in new TestCase {
        val commonSameAs = datasetExternalSameAs.toGenerateOfFixedValue
        val (dataset1, project1) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne

        upload(
          to = renkuDataset,
          project1,
          project2,
          anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceNonModified)).generateOne._2
        )

        datasetFinder
          .findDataset(dataset1.identifier, AuthContext(None, dataset1.identifier, Set(project1.path)))
          .unsafeRunSync() shouldBe
          importedExternalToNonModified(dataset1, project1)
            .copy(usedIn = List(project1.to[DatasetProject], project2.to[DatasetProject]).sorted)
            .some

        datasetFinder
          .findDataset(dataset2.identifier, AuthContext(None, dataset2.identifier, Set(project2.path)))
          .unsafeRunSync() shouldBe
          importedExternalToNonModified(dataset2, project2)
            .copy(usedIn = List(project1.to[DatasetProject], project2.to[DatasetProject]).sorted)
            .some
      }

    "return details of the dataset with the given id " +
      "- a case when dataset is modified" in new TestCase {
        val commonSameAs = datasetExternalSameAs.toGenerateOfFixedValue
        val (_ ::~ dataset1Modified, project1) = anyRenkuProjectEntities(visibilityPublic)
          .addDatasetAndModification(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne

        upload(to = renkuDataset, project1, project2)

        datasetFinder
          .findDataset(dataset2.identifier, AuthContext(None, dataset2.identifier, Set(project2.path)))
          .unsafeRunSync() shouldBe importedExternalToNonModified(dataset2, project2).some

        datasetFinder
          .findDataset(dataset1Modified.identifier, AuthContext(None, dataset1Modified.identifier, Set(project1.path)))
          .unsafeRunSync() shouldBe modifiedToModified(dataset1Modified, project1).some
      }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are using the same dataset created in a Renku project" in new TestCase {
        forAll(anyRenkuProjectEntities(visibilityPublic) addDataset datasetEntities(provenanceInternal)) {
          case (sourceDataset, sourceProject) =>
            val (_, project1)        = (renkuProjectEntities(visibilityPublic) importDataset sourceDataset).generateOne
            val (dataset2, project2) = (renkuProjectEntities(visibilityPublic) importDataset sourceDataset).generateOne

            upload(to = renkuDataset, sourceProject, project1, project2)

            datasetFinder
              .findDataset(sourceDataset.identifier,
                           AuthContext(None, sourceDataset.identifier, Set(sourceProject.path))
              )
              .unsafeRunSync() shouldBe
              internalToNonModified(sourceDataset, sourceProject)
                .copy(
                  usedIn = List(sourceProject.to[DatasetProject],
                                project1.to[DatasetProject],
                                project2.to[DatasetProject]
                  ).sorted
                )
                .some

            datasetFinder
              .findDataset(dataset2.identifier, AuthContext(None, dataset2.identifier, Set(project2.path)))
              .unsafeRunSync() shouldBe
              importedInternalToNonModified(dataset2, project2)
                .copy(
                  usedIn = List(sourceProject.to[DatasetProject],
                                project1.to[DatasetProject],
                                project2.to[DatasetProject]
                  ).sorted
                )
                .some
        }
      }

    "return None if there are no datasets with the given id" in new TestCase {
      val id = datasetIdentifiers.generateOne
      datasetFinder.findDataset(id, AuthContext(None, id, Set.empty)).unsafeRunSync() shouldBe None
    }

    "return None if dataset was invalidated" in new TestCase {

      val (original ::~ invalidation, project) =
        (renkuProjectEntities(visibilityPublic) addDatasetAndInvalidation datasetEntities(
          provenanceInternal
        )).generateOne

      upload(to = renkuDataset, project)

      datasetFinder
        .findDataset(original.identifier, AuthContext(None, original.identifier, Set(project.path)))
        .unsafeRunSync() shouldBe None
      datasetFinder
        .findDataset(invalidation.identifier, AuthContext(None, invalidation.identifier, Set(project.path)))
        .unsafeRunSync() shouldBe None
    }

    "return a dataset without invalidated part" in new TestCase {
      val (dataset, project) = anyRenkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceInternal)
            .modify { ds =>
              ds.copy(parts = datasetPartEntities(ds.provenance.date.instant).generateNonEmptyList().toList)
            }
        )
        .generateOne
      val partToInvalidate           = Random.shuffle(dataset.parts).head
      val datasetWithInvalidatedPart = dataset.invalidatePartNow(partToInvalidate)
      val projectBothDatasets        = project.addDatasets(datasetWithInvalidatedPart)

      upload(to = renkuDataset, projectBothDatasets)

      datasetFinder
        .findDataset(dataset.identifier, AuthContext(None, dataset.identifier, Set(project.path)))
        .unsafeRunSync() shouldBe
        internalToNonModified(dataset, project).copy(usedIn = Nil).some

      datasetFinder
        .findDataset(datasetWithInvalidatedPart.identifier,
                     AuthContext(None, datasetWithInvalidatedPart.identifier, Set(projectBothDatasets.path))
        )
        .unsafeRunSync() shouldBe
        modifiedToModified(datasetWithInvalidatedPart, projectBothDatasets).some
    }

    "not return dataset if user is not authorised to the project where the DS belongs to" in new TestCase {
      val (dataset, project) =
        (anyRenkuProjectEntities(visibilityPublic) addDataset datasetEntities(provenanceInternal)).generateOne

      upload(to = renkuDataset, project)

      datasetFinder
        .findDataset(dataset.identifier, AuthContext(None, dataset.identifier, Set(projectPaths.generateOne)))
        .unsafeRunSync() shouldBe None
    }

    "return dataset without usedIn to which the user has no access to" in new TestCase {
      val authUser = authUsers.generateOne
      val (dataset, project) = (renkuProjectEntities(visibilityNonPublic).map(
        _.copy(members = Set(personEntities.generateOne.copy(maybeGitLabId = authUser.id.some)))
      ) addDataset datasetEntities(provenanceInternal)).generateOne
      val (_, otherProject) = renkuProjectEntities(visibilityNonPublic)
        .map(_.copy(members = personEntities(withGitLabId).generateSet()))
        .importDataset(dataset)
        .generateOne

      upload(to = renkuDataset, project, otherProject)

      datasetFinder
        .findDataset(dataset.identifier, AuthContext(authUser.some, dataset.identifier, Set(project.path)))
        .unsafeRunSync() shouldBe internalToNonModified(dataset, project).some
    }

    "return dataset with usedIn to which the user has access to" in new TestCase {
      val authUser = authUsers.generateOne
      val (dataset, project) = (renkuProjectEntities(visibilityNonPublic).map(
        _.copy(members = Set(personEntities.generateOne.copy(maybeGitLabId = authUser.id.some)))
      ) addDataset datasetEntities(provenanceInternal)).generateOne
      val (_, otherProject) = renkuProjectEntities(visibilityNonPublic)
        .map(_.copy(members = personEntities(withGitLabId).generateSet() ++ project.members))
        .importDataset(dataset)
        .generateOne

      upload(to = renkuDataset, project, otherProject)

      datasetFinder
        .findDataset(dataset.identifier,
                     AuthContext(authUser.some, dataset.identifier, Set(project.path, otherProject.path))
        )
        .unsafeRunSync() shouldBe internalToNonModified(dataset, project)
        .copy(usedIn = List(project.to[DatasetProject], otherProject.to[DatasetProject]).sorted)
        .some
    }
  }

  "findDataset in case of forks" should {

    "return details of the dataset with the given id " +
      "- a case when a Renku created dataset is defined on a project which has a fork" in new TestCase {
        val (originalDataset, originalProject ::~ fork) = renkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceInternal))
          .forkOnce()
          .generateOne

        upload(to = renkuDataset, originalProject, fork)

        assume(originalProject.datasets === fork.datasets,
               "Datasets on original project and its fork should be the same"
        )

        datasetFinder
          .findDataset(originalDataset.identifier,
                       AuthContext(None, originalDataset.identifier, Set(originalProject.path))
          )
          .unsafeRunSync() shouldBe
          internalToNonModified(originalDataset, originalProject)
            .copy(usedIn = List(originalProject.to[DatasetProject], fork.to[DatasetProject]).sorted)
            .some
      }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are sharing a dataset and one of the projects is forked" in new TestCase {
        val commonSameAs = datasetExternalSameAs.toGenerateOfFixedValue
        val (dataset1, project1) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne
        val (dataset2, project2 ::~ project2Fork) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .forkOnce()
          .generateOne

        upload(to = renkuDataset, project1, project2, project2Fork)

        datasetFinder
          .findDataset(dataset1.identifier, AuthContext(None, dataset1.identifier, Set(project1.path)))
          .unsafeRunSync() shouldBe
          importedExternalToNonModified(dataset1, project1)
            .copy(usedIn =
              List(project1.to[DatasetProject], project2.to[DatasetProject], project2Fork.to[DatasetProject]).sorted
            )
            .some

        assume(project2.datasets === project2Fork.datasets, "Datasets on original project and fork should be the same")

        datasetFinder
          .findDataset(dataset2.identifier, AuthContext(None, dataset2.identifier, Set(project2.path)))
          .unsafeRunSync() shouldBe
          importedExternalToNonModified(dataset2, project2)
            .copy(usedIn =
              List(project1.to[DatasetProject], project2.to[DatasetProject], project2Fork.to[DatasetProject]).sorted
            )
            .some
      }

    "return details of the dataset with the given id " +
      "- a case when a Renku created dataset is defined on a grandparent project with two levels of forks" in new TestCase {
        forAll(anyRenkuProjectEntities(visibilityPublic) addDataset datasetEntities(provenanceInternal)) {
          case dataset ::~ grandparent =>
            val grandparentForked ::~ parent = grandparent.forkOnce()
            val parentForked ::~ child       = parent.forkOnce()

            upload(to = renkuDataset, grandparentForked, parentForked, child)

            assume(
              (grandparentForked.datasets === parentForked.datasets) && (parentForked.datasets === child.datasets),
              "Datasets on original project and forks have to be the same"
            )

            datasetFinder
              .findDataset(dataset.identifier, AuthContext(None, dataset.identifier, Set(grandparentForked.path)))
              .unsafeRunSync() shouldBe
              internalToNonModified(dataset, grandparentForked)
                .copy(usedIn =
                  List(grandparentForked.to[DatasetProject],
                       parentForked.to[DatasetProject],
                       child.to[DatasetProject]
                  ).sorted
                )
                .some
        }
      }

    "return details of the modified dataset with the given id " +
      "- case when modification is followed by forking" in new TestCase {
        forAll(
          anyRenkuProjectEntities(visibilityPublic)
            .addDatasetAndModification(datasetEntities(provenanceInternal))
            .forkOnce()
        ) { case (original ::~ modified, project ::~ fork) =>
          upload(to = renkuDataset, project, fork)

          datasetFinder
            .findDataset(original.identifier, AuthContext(None, original.identifier, Set(project.path)))
            .unsafeRunSync() shouldBe
            internalToNonModified(original, project).copy(usedIn = Nil).some

          datasetFinder
            .findDataset(modified.identifier, AuthContext(None, modified.identifier, Set(project.path)))
            .unsafeRunSync() shouldBe
            modifiedToModified(modified, project)
              .copy(usedIn = List(project.to[DatasetProject], fork.to[DatasetProject]).sorted)
              .some
        }
      }

    "return details of the modified dataset with the given id " +
      "- case when modification is followed by forking and some other modification" in new TestCase {
        val (original ::~ modified, project ::~ fork) = anyRenkuProjectEntities(visibilityPublic)
          .addDatasetAndModification(datasetEntities(provenanceInternal))
          .forkOnce()
          .generateOne
        val (modifiedAgain, projectUpdated) = project.addDataset(modified.createModification())

        upload(to = renkuDataset, projectUpdated, fork)

        datasetFinder
          .findDataset(original.identifier, AuthContext(None, original.identifier, Set(projectUpdated.path)))
          .unsafeRunSync() shouldBe
          internalToNonModified(original, projectUpdated).copy(usedIn = Nil).some

        datasetFinder
          .findDataset(modified.identifier, AuthContext(None, modified.identifier, Set(projectUpdated.path)))
          .unsafeRunSync() shouldBe
          modifiedToModified(modified, projectUpdated).copy(usedIn = List(fork.to[DatasetProject])).some

        datasetFinder
          .findDataset(modifiedAgain.identifier, AuthContext(None, modifiedAgain.identifier, Set(projectUpdated.path)))
          .unsafeRunSync() shouldBe
          modifiedToModified(modifiedAgain, projectUpdated).some
      }

    "return details of the dataset with the given id " +
      "- case when forking is followed by modification" in new TestCase {
        forAll(anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceInternal)).forkOnce()) {
          case (original, project ::~ fork) =>
            val (modifiedOnFork, forkUpdated) = fork.addDataset(original.createModification())

            upload(to = renkuDataset, project, forkUpdated)

            datasetFinder
              .findDataset(original.identifier, AuthContext(None, original.identifier, Set(project.path)))
              .unsafeRunSync() shouldBe
              internalToNonModified(original, project).some

            datasetFinder
              .findDataset(modifiedOnFork.identifier,
                           AuthContext(None, modifiedOnFork.identifier, Set(forkUpdated.path))
              )
              .unsafeRunSync() shouldBe
              modifiedToModified(modifiedOnFork, forkUpdated).some
        }
      }

    "return details of the dataset with the given id " +
      "- case when a dataset on a fork is deleted" in new TestCase {
        forAll(anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceInternal)).forkOnce()) {
          case (original, project ::~ fork) =>
            val invalidation         = original.invalidateNow
            val forkWithInvalidation = fork.addDatasets(invalidation)

            upload(to = renkuDataset, project, forkWithInvalidation)

            datasetFinder
              .findDataset(original.identifier, AuthContext(None, original.identifier, Set(project.path)))
              .unsafeRunSync() shouldBe
              internalToNonModified(original, project).some

            datasetFinder
              .findDataset(invalidation.identifier, AuthContext(None, invalidation.identifier, Set(project.path)))
              .unsafeRunSync() shouldBe None
        }
      }

    "return details of a fork dataset with the given id " +
      "- case when the dataset on the parent is deleted" in new TestCase {
        val (original, project ::~ fork) =
          anyRenkuProjectEntities(visibilityPublic)
            .addDataset(datasetEntities(provenanceInternal))
            .forkOnce()
            .generateOne
        val invalidation            = original.invalidateNow
        val projectWithInvalidation = project.addDatasets(invalidation)

        upload(to = renkuDataset, projectWithInvalidation, fork)

        datasetFinder
          .findDataset(original.identifier, AuthContext(None, original.identifier, Set(fork.path)))
          .unsafeRunSync() shouldBe
          internalToNonModified(original, fork).some

        datasetFinder
          .findDataset(invalidation.identifier, AuthContext(None, invalidation.identifier, Set(fork.path)))
          .unsafeRunSync() shouldBe None
      }

    "return details of the dataset with the given id " +
      "- a case when the user has no access to the original project" in new TestCase {
        val (originalDataset, originalProject ::~ fork) = renkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceInternal))
          .forkOnce()
          .generateOne

        upload(to = renkuDataset, originalProject, fork)

        assume(originalProject.datasets === fork.datasets,
               "Datasets on original project and its fork should be the same"
        )

        datasetFinder
          .findDataset(originalDataset.identifier, AuthContext(None, originalDataset.identifier, Set(fork.path)))
          .unsafeRunSync() shouldBe
          internalToNonModified(originalDataset, fork)
            .copy(usedIn = List(originalProject.to[DatasetProject], fork.to[DatasetProject]).sorted)
            .some
      }
  }

  "findDataset in the case of dataset import hierarchy" should {

    "return details of the dataset with the given id " +
      "- case when the first dataset is imported from a third party provider" in new TestCase {
        val commonSameAs = datasetExternalSameAs.toGenerateOfFixedValue
        val (dataset1, project1) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne
        val (dataset3, project3) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset2).generateOne

        upload(to = renkuDataset, project1, project2, project3)

        datasetFinder
          .findDataset(dataset1.identifier, AuthContext(None, dataset1.identifier, Set(project1.path)))
          .unsafeRunSync() shouldBe
          importedExternalToNonModified(dataset1, project1)
            .copy(usedIn = List(project1, project2, project3).map(_.to[DatasetProject]).sorted)
            .some

        datasetFinder
          .findDataset(dataset3.identifier, AuthContext(None, dataset3.identifier, Set(project3.path)))
          .unsafeRunSync() shouldBe
          importedInternalToNonModified(dataset3, project3)
            .copy(
              sameAs = dataset2.provenance.sameAs,
              usedIn = List(project1, project2, project3).map(_.to[DatasetProject]).sorted
            )
            .some
      }

    "return details of the dataset with the given id " +
      "- case when the first dataset is renku created" in new TestCase {
        val (dataset1, project1) =
          anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceInternal)).generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset1).generateOne
        val (dataset3, project3) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset2).generateOne

        upload(to = renkuDataset, project1, project2, project3)

        datasetFinder
          .findDataset(dataset1.identifier, AuthContext(None, dataset1.identifier, Set(project1.path)))
          .unsafeRunSync() shouldBe
          internalToNonModified(dataset1, project1)
            .copy(usedIn = List(project1, project2, project3).map(_.to[DatasetProject]).sorted)
            .some

        datasetFinder
          .findDataset(dataset3.identifier, AuthContext(None, dataset3.identifier, Set(project3.path)))
          .unsafeRunSync() shouldBe
          importedInternalToNonModified(dataset3, project3)
            .copy(
              sameAs = SameAs(dataset1.provenance.entityId),
              usedIn = List(project1, project2, project3).map(_.to[DatasetProject]).sorted
            )
            .some
      }

    "return details of the dataset with the given id " +
      "- case when the sameAs hierarchy is broken by dataset modification" in new TestCase {
        val (dataset1, project1) =
          anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceInternal)).generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset1).generateOne
        val (dataset2Modified, project2Updated) = project2.addDataset(dataset2.createModification())
        val (_, project3) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset2Modified).generateOne

        upload(to = renkuDataset, project1, project2Updated, project3)

        datasetFinder
          .findDataset(dataset1.identifier, AuthContext(None, dataset1.identifier, Set(project1.path)))
          .unsafeRunSync() shouldBe
          internalToNonModified(dataset1, project1).some
        datasetFinder
          .findDataset(dataset2Modified.identifier,
                       AuthContext(None, dataset2Modified.identifier, Set(project2Updated.path))
          )
          .unsafeRunSync() shouldBe
          modifiedToModified(dataset2Modified, project2Updated)
            .copy(usedIn = List(project2Updated.to[DatasetProject], project3.to[DatasetProject]).sorted)
            .some
      }

    "not return the details of a dataset" +
      "- case when the latest import of the dataset has been invalidated" in new TestCase {
        val (dataset1, project1) =
          anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceInternal)).generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset1).generateOne
        val dataset2Invalidation = dataset2.invalidateNow
        val project2Updated      = project2.addDatasets(dataset2Invalidation)

        upload(to = renkuDataset, project1, project2Updated)

        datasetFinder
          .findDataset(dataset1.identifier, AuthContext(None, dataset1.identifier, Set(project1.path)))
          .unsafeRunSync() shouldBe
          internalToNonModified(dataset1, project1).some
        datasetFinder
          .findDataset(dataset2.identifier, AuthContext(None, dataset2.identifier, Set(project2.path)))
          .unsafeRunSync() shouldBe None
        datasetFinder
          .findDataset(dataset2Invalidation.identifier,
                       AuthContext(None, dataset2Invalidation.identifier, Set(project2.path))
          )
          .unsafeRunSync() shouldBe None
      }

    "not return the details of a dataset" +
      "- case when the original dataset has been invalidated" in new TestCase {

        val (dataset1, project1) =
          anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceInternal)).generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset1).generateOne
        val dataset1Invalidation = dataset1.invalidateNow
        val project1Updated      = project1.addDatasets(dataset1Invalidation)

        upload(to = renkuDataset, project1Updated, project2)

        datasetFinder
          .findDataset(dataset1.identifier, AuthContext(None, dataset1.identifier, Set(project1.path)))
          .unsafeRunSync() shouldBe None
        datasetFinder
          .findDataset(dataset1Invalidation.identifier,
                       AuthContext(None, dataset1Invalidation.identifier, Set(project1.path))
          )
          .unsafeRunSync() shouldBe None
        datasetFinder
          .findDataset(dataset2.identifier, AuthContext(None, dataset2.identifier, Set(project2.path)))
          .unsafeRunSync() shouldBe importedInternalToNonModified(dataset2, project2).some
      }

    "not return the details of a dataset" +
      "- case when the latest modification of the dataset has been invalidated" in new TestCase {
        val (dataset ::~ datasetModified, project) = anyRenkuProjectEntities(visibilityPublic)
          .addDatasetAndModification(datasetEntities(provenanceInternal))
          .generateOne
        val datasetInvalidation = datasetModified.invalidateNow
        val projectUpdated      = project.addDatasets(datasetInvalidation)

        upload(to = renkuDataset, projectUpdated)

        datasetFinder
          .findDataset(dataset.identifier, AuthContext(None, dataset.identifier, Set(project.path)))
          .unsafeRunSync() shouldBe internalToNonModified(dataset, projectUpdated).copy(usedIn = Nil).some
        datasetFinder
          .findDataset(datasetModified.identifier, AuthContext(None, datasetModified.identifier, Set(project.path)))
          .unsafeRunSync() shouldBe None
        datasetFinder
          .findDataset(datasetInvalidation.identifier,
                       AuthContext(None, datasetInvalidation.identifier, Set(projectUpdated.path))
          )
          .unsafeRunSync() shouldBe None
      }
  }

  private trait TestCase {
    implicit val renkuUrl:             RenkuUrl                    = renkuUrls.generateOne
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val datasetFinder = new DatasetFinderImpl[IO](
      new BaseDetailsFinderImpl[IO](renkuDSConnectionInfo),
      new CreatorsFinderImpl[IO](renkuDSConnectionInfo),
      new PartsFinderImpl[IO](renkuDSConnectionInfo),
      new ProjectsFinderImpl[IO](renkuDSConnectionInfo)
    )
  }

  private implicit lazy val usedInOrdering: Ordering[DatasetProject] = Ordering.by(_.name)
}
