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

package io.renku.knowledgegraph.datasets
package details

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.countingGen
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.datasets.{Identifier, SameAs, TopmostSameAs}
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.testentities._
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.datasets.details.Dataset._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.{GraphJenaSpec, ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class DatasetFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with TestSearchInfoDatasets
    with ScalaCheckPropertyChecks
    with should.Matchers
    with OptionValues {

  "findDataset" should {

    forAll(
      anyRenkuProjectEntities(visibilityPublic) addDataset datasetEntities(provenanceInternal),
      anyRenkuProjectEntities(visibilityPublic) addDataset datasetEntities(provenanceNonModified),
      countingGen
    ) { case ((dataset, project), (_, otherProject), attempt) =>
      "return details of the dataset with the given id " +
        s"- a case of a non-modified renku dataset used in a single project #$attempt" in projectsDSConfig.use {
          implicit pcc =>
            for {
              _ <- provisionTestProjects(project, otherProject)

              expected = internalToNonModified(dataset, project)

              _ <- findById(dataset.identifier).asserting(_.value shouldBe expected)
              _ <- findByTopmostSameAs(dataset.provenance.topmostSameAs).asserting(_.value shouldBe expected)
            } yield Succeeded
        }
    }

    "return details of the dataset with the given id " +
      "- a case when unrelated projects are using the same External dataset" in projectsDSConfig.use { implicit pcc =>
        val commonSameAs = datasetExternalSameAs.generateOne
        val (dataset1, project1) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne

        for {
          _ <-
            provisionTestProjects(
              project1,
              project2,
              anyRenkuProjectEntities(visibilityPublic).withDatasets(datasetEntities(provenanceNonModified)).generateOne
            )

          expectedDS1 =
            importedExternalToNonModified(dataset1, project1)
              .copy(usedIn = List(toDatasetProject(project1, dataset1), toDatasetProject(project2, dataset2)).sorted)
          _ <- findById(dataset1.identifier).asserting(_.value shouldBe expectedDS1)

          expectedDS2 =
            importedExternalToNonModified(dataset2, project2)
              .copy(usedIn = List(toDatasetProject(project1, dataset1), toDatasetProject(project2, dataset2)).sorted)
          _ <- findById(dataset2.identifier).asserting(_.value shouldBe expectedDS2)

          _ <- findBySameAs(commonSameAs).asserting(_.value should (be(expectedDS1) or be(expectedDS2)))
        } yield Succeeded
      }

    "return details of the dataset with the given id " +
      "- a case where the dataset is modified" in projectsDSConfig.use { implicit pcc =>
        val commonSameAs = datasetExternalSameAs.generateOne
        val (originalDs -> dataset1Modified, project1) = anyRenkuProjectEntities(visibilityPublic)
          .addDatasetAndModification(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne

        for {
          _ <- provisionTestProjects(project1, project2)

          expectedDS2 = importedExternalToNonModified(dataset2, project2)
          _ <- findById(dataset2.identifier).asserting(_.value shouldBe expectedDS2)
          _ <- findBySameAs(commonSameAs).asserting(_.value shouldBe expectedDS2)

          expectedDS1Modified = modifiedToModified(dataset1Modified, originalDs.provenance.date, project1)
          _ <- findById(dataset1Modified.identifier).asserting(_.value shouldBe expectedDS1Modified)
          _ <- findByTopmostSameAs(dataset1Modified.provenance.topmostSameAs)
                 .asserting(_.value shouldBe expectedDS1Modified)
        } yield Succeeded
      }

    "return details of the dataset with the given id " +
      "- a case where unrelated projects are using the same Internal dataset" in projectsDSConfig.use { implicit pcc =>
        val sourceDataset -> sourceProject =
          anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceInternal)).generateOne
        val (dataset1, project1) = renkuProjectEntities(visibilityPublic).importDataset(sourceDataset).generateOne
        val (dataset2, project2) = renkuProjectEntities(visibilityPublic).importDataset(sourceDataset).generateOne

        for {
          _ <- provisionTestProjects(sourceProject, project1, project2)

          expectedSourceDS = internalToNonModified(sourceDataset, sourceProject)
                               .copy(usedIn =
                                 List(toDatasetProject(sourceProject, sourceDataset),
                                      toDatasetProject(project1, dataset1),
                                      toDatasetProject(project2, dataset2)
                                 ).sorted
                               )
          _ <- findById(sourceDataset.identifier).asserting(_.value shouldBe expectedSourceDS)

          expectedDS2 = importedInternalToNonModified(dataset2, project2)
                          .copy(usedIn = expectedSourceDS.usedIn)
          _ <- findById(dataset2.identifier).asserting(_.value shouldBe expectedDS2)

          expectedDS1 = importedInternalToNonModified(dataset1, project1)
                          .copy(usedIn = expectedSourceDS.usedIn)
          _ <- findBySameAs(SameAs(sourceDataset.entityId)).asserting {
                 _.value should (be(expectedSourceDS) or be(expectedDS2) or be(expectedDS1))
               }
        } yield Succeeded
      }

    "return None if there are no datasets with the given id" in projectsDSConfig.use { implicit pcc =>
      findById(datasetIdentifiers.generateOne).asserting(_ shouldBe None) >>
        findBySameAs(datasetSameAs.generateOne).asserting(_ shouldBe None)
    }

    "return None if dataset was invalidated" in projectsDSConfig.use { implicit pcc =>
      val (original -> invalidation, project) =
        renkuProjectEntities(visibilityPublic)
          .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
          .generateOne

      for {
        _ <- provisionTestProject(project)

        _ <- findById(original.identifier).asserting(_ shouldBe None)
        _ <- findByTopmostSameAs(original.provenance.topmostSameAs).asserting(_ shouldBe None)
        _ <- findById(invalidation.identifier).asserting(_ shouldBe None)
        _ <- findByTopmostSameAs(invalidation.provenance.topmostSameAs).asserting(_ shouldBe None)
      } yield Succeeded
    }

    "return a dataset without invalidated part" in projectsDSConfig.use { implicit pcc =>
      val dataset -> project = anyRenkuProjectEntities(visibilityPublic)
        .addDataset(
          datasetEntities(provenanceInternal)
            .modify { ds =>
              ds.copy(parts = datasetPartEntities(ds.provenance.date.instant).generateNonEmptyList().toList)
            }
        )
        .generateOne

      val partToInvalidate           = Random.shuffle(dataset.parts).head
      val datasetWithInvalidatedPart = dataset.invalidatePartNow(partToInvalidate, personEntities)
      val projectBothDatasets        = project.addDatasets(datasetWithInvalidatedPart)

      for {
        _ <- provisionTestProject(projectBothDatasets)

        expectedDS = internalToNonModified(dataset, project).copy(usedIn = Nil)
        _ <- findById(dataset.identifier).asserting(_.value shouldBe expectedDS)
        _ <- findByTopmostSameAs(dataset.provenance.topmostSameAs).asserting(_ shouldBe None)

        expectedDSWithInvalidatedPart =
          modifiedToModified(datasetWithInvalidatedPart, dataset.provenance.date, projectBothDatasets)
        _ <- findById(datasetWithInvalidatedPart.identifier).asserting(_.value shouldBe expectedDSWithInvalidatedPart)
        _ <- findByTopmostSameAs(datasetWithInvalidatedPart.provenance.topmostSameAs)
               .asserting(_.value shouldBe expectedDSWithInvalidatedPart)
      } yield Succeeded
    }

    "not return a dataset if the user is not authorised for the project where the DS belongs to" in projectsDSConfig
      .use { implicit pcc =>
        val (dataset, project) =
          anyRenkuProjectEntities(visibilityPrivate).addDataset(datasetEntities(provenanceInternal)).generateOne

        for {
          _ <- provisionTestProject(project)

          _ <- findById(dataset.identifier).asserting(_ shouldBe None)
          _ <- findByTopmostSameAs(dataset.provenance.topmostSameAs).asserting(_ shouldBe None)
        } yield Succeeded
      }

    "return dataset without usedIn to which the user has no access" in projectsDSConfig.use { implicit pcc =>
      val authUser = authUsers.generateOne
      val (dataset, project) = renkuProjectEntities(visibilityNonPublic)
        .map(replaceMembers(Set(projectMemberEntities(authUser.id.some).generateOne)))
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
      val (_, otherProject) = renkuProjectEntities(visibilityPrivate)
        .map(replaceMembers(Set(projectMemberEntities(withGitLabId).generateOne)))
        .importDataset(dataset)
        .generateOne

      for {
        _ <- provisionTestProjects(project, otherProject)

        expectedDS = internalToNonModified(dataset, project)
        _ <- findById(dataset.identifier, authUser).asserting(_.value shouldBe expectedDS)
        _ <- findByTopmostSameAs(dataset.provenance.topmostSameAs, authUser).asserting(_.value shouldBe expectedDS)
      } yield Succeeded
    }

    "return dataset with usedIn to which the user has access" in projectsDSConfig.use { implicit pcc =>
      val authUser = authUsers.generateOne
      val (dataset, project) = renkuProjectEntities(visibilityNonPublic)
        .map(replaceMembers(Set(projectMemberEntities(authUser.id.some).generateOne)))
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
      val (otherDS, otherProject) = renkuProjectEntities(visibilityNonPublic)
        .map(replaceMembers(projectMemberEntities(withGitLabId).generateSet() ++ project.members))
        .importDataset(dataset)
        .generateOne

      for {
        _ <- provisionTestProjects(project, otherProject)

        expectedUsedIns = List(toDatasetProject(project, dataset), toDatasetProject(otherProject, otherDS)).sorted
        expectedDS      = internalToNonModified(dataset, project).copy(usedIn = expectedUsedIns)
        expectedOtherDS = importedInternalToNonModified(otherDS, otherProject).copy(usedIn = expectedUsedIns)
        _ <- findById(dataset.identifier, authUser).asserting(_.value shouldBe expectedDS)
        _ <- findByTopmostSameAs(dataset.provenance.topmostSameAs, authUser)
               .asserting(_.value should (be(expectedDS) or be(expectedOtherDS)))
      } yield Succeeded
    }
  }

  "findDataset in case of forks" should {

    "return details of the dataset with the given id " +
      "- a case where an Internal dataset is defined on a project which has a fork" in projectsDSConfig.use {
        implicit pcc =>
          val (originalDataset, originalProject -> fork) = renkuProjectEntities(visibilityPublic)
            .addDataset(datasetEntities(provenanceInternal))
            .forkOnce()
            .generateOne

          for {
            _ <- provisionTestProjects(originalProject, fork)

            _ = assume(originalProject.datasets === fork.datasets,
                       "Datasets on original project and its fork should be the same"
                )

            expectedDS = internalToNonModified(originalDataset, originalProject)
                           .copy(usedIn =
                             List(toDatasetProject(originalProject, originalDataset),
                                  toDatasetProject(fork, originalDataset)
                             ).sorted
                           )
            _ <- findById(originalDataset.identifier).asserting(_.value shouldBe expectedDS)
            _ <- findByTopmostSameAs(originalDataset.provenance.topmostSameAs).asserting(_.value shouldBe expectedDS)
          } yield Succeeded
      }

    "return details of the dataset with the given id " +
      "- a case where unrelated projects are sharing a dataset and one of has a fork" in projectsDSConfig.use {
        implicit pcc =>
          val commonSameAs = datasetExternalSameAs.generateOne
          val (dataset1, project1) = anyRenkuProjectEntities(visibilityPublic)
            .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
            .generateOne
          val (dataset2, project2 -> project2Fork) = anyRenkuProjectEntities(visibilityPublic)
            .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
            .forkOnce()
            .generateOne

          for {
            _ <- provisionTestProjects(project1, project2, project2Fork)

            expectedUsedIns = List(toDatasetProject(project1, dataset1),
                                   toDatasetProject(project2, dataset2),
                                   toDatasetProject(project2Fork, dataset2)
                              ).sorted
            expectedDS1 = importedExternalToNonModified(dataset1, project1).copy(usedIn = expectedUsedIns.sorted)
            _ <- findById(dataset1.identifier).asserting(_.value shouldBe expectedDS1)

            _ = assume(project2.datasets === project2Fork.datasets,
                       "Datasets on original project and fork should be the same"
                )

            expectedDS2 = importedExternalToNonModified(dataset2, project2).copy(usedIn = expectedUsedIns.sorted)
            _ <- findById(dataset2.identifier).asserting(_.value shouldBe expectedDS2)

            expectedDS2Fork =
              importedExternalToNonModified(dataset2, project2Fork).copy(usedIn = expectedUsedIns.sorted)
            _ <- findBySameAs(commonSameAs).asserting {
                   _.value should (be(expectedDS1) or be(expectedDS2) or be(expectedDS2Fork))
                 }
          } yield Succeeded
      }

    "return details of the dataset with the given id " +
      "- a case where an Internal dataset is defined on a grandparent project with two levels of forks" in projectsDSConfig
        .use { implicit pcc =>
          val dataset -> grandparent =
            anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceInternal)).generateOne
          val grandparentForked -> parent = grandparent.forkOnce()
          val parentForked -> child       = parent.forkOnce()

          for {
            _ <- provisionTestProjects(grandparentForked, parentForked, child)

            _ = assume(
                  (grandparentForked.datasets === parentForked.datasets) && (parentForked.datasets === child.datasets),
                  "Datasets on original project and forks have to be the same"
                )

            expectedDS = internalToNonModified(dataset, grandparentForked)
                           .copy(usedIn =
                             List(toDatasetProject(grandparentForked, dataset),
                                  toDatasetProject(parentForked, dataset),
                                  toDatasetProject(child, dataset)
                             ).sorted
                           )
            _ <- findById(dataset.identifier).asserting(_.value shouldBe expectedDS)
            _ <- findByTopmostSameAs(dataset.provenance.topmostSameAs).asserting(_.value shouldBe expectedDS)
          } yield Succeeded
        }

    "return details of the modified dataset with the given id " +
      "- case where modification is followed by forking" in projectsDSConfig.use { implicit pcc =>
        val (original -> modified, project -> fork) =
          anyRenkuProjectEntities(visibilityPublic)
            .addDatasetAndModification(datasetEntities(provenanceInternal))
            .forkOnce()
            .generateOne

        for {
          _ <- provisionTestProjects(project, fork)

          _ <- findById(original.identifier).asserting(
                 _.value shouldBe internalToNonModified(original, project).copy(usedIn = Nil)
               )

          expectedUsedIns = List(toDatasetProject(project, modified), toDatasetProject(fork, modified)).sorted
          expectedModified =
            modifiedToModified(modified, original.provenance.date, project).copy(usedIn = expectedUsedIns)
          _ <- findById(modified.identifier).asserting(_.value shouldBe expectedModified)

          expectedModifiedFork =
            modifiedToModified(modified, original.provenance.date, fork).copy(usedIn = expectedUsedIns)
          _ <- findByTopmostSameAs(modified.provenance.topmostSameAs)
                 .asserting(_.value should (be(expectedModified) or be(expectedModifiedFork)))
        } yield Succeeded
      }

    "return details of the modified dataset with the given id " +
      "- case where modification is followed by forking and some other modification" in projectsDSConfig.use {
        implicit pcc =>
          val (original -> modified, project -> fork) = anyRenkuProjectEntities(visibilityPrivate)
            .addDatasetAndModification(datasetEntities(provenanceInternal))
            .forkOnce()
            .generateOne
          val (modifiedAgain, projectUpdated0) = project.addDataset(modified.createModification())
          val projectUpdated                   = replaceVisibility(Visibility.Public).apply(projectUpdated0)

          for {
            _ <- provisionTestProjects(projectUpdated, fork)

            _ <- findById(original.identifier).asserting(
                   _.value shouldBe internalToNonModified(original, projectUpdated).copy(usedIn = Nil)
                 )
            _ <- findByTopmostSameAs(original.provenance.topmostSameAs).asserting(_ shouldBe None)

            expectedModified =
              modifiedToModified(modified, original.provenance.date, projectUpdated)
                .copy(usedIn = Nil /*List(toDatasetProject(fork, modified)) */ )
            _ <- findById(modified.identifier).asserting(_.value shouldBe expectedModified)
            _ <- findByTopmostSameAs(modified.provenance.topmostSameAs).asserting(_ shouldBe None)

            expectedLastModification = modifiedToModified(modifiedAgain, original.provenance.date, projectUpdated)
            _ <- findById(modifiedAgain.identifier).asserting(_.value shouldBe expectedLastModification)
            _ <- findByTopmostSameAs(modifiedAgain.provenance.topmostSameAs)
                   .asserting(_.value shouldBe expectedLastModification)
          } yield Succeeded
      }

    "return details of the dataset with the given id " +
      "- case where forking is followed by modification" in projectsDSConfig.use { implicit pcc =>
        val (original, project -> fork) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceInternal))
          .forkOnce()
          .generateOne
        val (modifiedOnFork, forkUpdated) = fork.addDataset(original.createModification())

        for {
          _ <- provisionTestProjects(project, forkUpdated)

          expectedOriginal = internalToNonModified(original, project)
          _ <- findById(original.identifier).asserting(_.value shouldBe expectedOriginal)
          _ <- findByTopmostSameAs(original.provenance.topmostSameAs).asserting(_.value shouldBe expectedOriginal)

          expectedModificationOnFork = modifiedToModified(modifiedOnFork, original.provenance.date, forkUpdated)
          _ <- findById(modifiedOnFork.identifier).asserting(_.value shouldBe expectedModificationOnFork)
          _ <- findByTopmostSameAs(modifiedOnFork.provenance.topmostSameAs)
                 .asserting(_.value shouldBe expectedModificationOnFork)
        } yield Succeeded
      }

    "return details of the dataset with the given id " +
      "- case when a dataset on a fork is deleted" in projectsDSConfig.use { implicit pcc =>
        val (original, project -> fork) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceInternal))
          .forkOnce()
          .generateOne
        val invalidation         = original.invalidateNow(personEntities)
        val forkWithInvalidation = fork.addDatasets(invalidation)

        for {
          _ <- provisionTestProjects(project, forkWithInvalidation)

          expectedOriginal = internalToNonModified(original, project)
          _ <- findById(original.identifier).asserting(_.value shouldBe expectedOriginal)
          _ <- findByTopmostSameAs(original.provenance.topmostSameAs).asserting(_.value shouldBe expectedOriginal)

          _ <- findById(invalidation.identifier).asserting(_ shouldBe None)
        } yield Succeeded
      }

    "return details of a fork dataset with the given id " +
      "- case where the dataset on the parent is deleted" in projectsDSConfig.use { implicit pcc =>
        val (original, project -> fork) =
          anyRenkuProjectEntities(visibilityPublic)
            .addDataset(datasetEntities(provenanceInternal))
            .forkOnce()
            .generateOne
        val invalidation            = original.invalidateNow(personEntities)
        val projectWithInvalidation = project.addDatasets(invalidation)

        for {
          _ <- provisionTestProjects(projectWithInvalidation, fork)

          expectedOriginal = internalToNonModified(original, fork)
          _ <- findById(original.identifier).asserting(_.value shouldBe expectedOriginal)
          _ <- findByTopmostSameAs(original.provenance.topmostSameAs).asserting(_.value shouldBe expectedOriginal)

          _ <- findById(invalidation.identifier).asserting(_ shouldBe None)
        } yield Succeeded
      }

    "return details of the dataset with the given id " +
      "- a case where the user has no access to the original project" in projectsDSConfig.use { implicit pcc =>
        val (originalDataset, originalProject -> fork) = renkuProjectEntities(visibilityPrivate)
          .addDataset(datasetEntities(provenanceInternal))
          .forkOnce()
          .generateOne
        val forkWithAccess =
          fork.copy(visibility = Visibility.Public)

        for {
          _ <- provisionTestProjects(originalProject, forkWithAccess)

          _ = assume(originalProject.datasets === forkWithAccess.datasets,
                     "Datasets on original project and its fork should be the same"
              )

          expectedDS = internalToNonModified(originalDataset, forkWithAccess)
                         .copy(usedIn = List(toDatasetProject(forkWithAccess, originalDataset)).sorted)
          _ <- findById(originalDataset.identifier).asserting(_.value shouldBe expectedDS)
          _ <- findByTopmostSameAs(originalDataset.provenance.topmostSameAs).asserting(_.value shouldBe expectedDS)
        } yield Succeeded
      }
  }

  "findDataset in the case of dataset import hierarchy" should {

    "return details of the dataset with the given id " +
      "- a case where the first dataset is an External dataset" in projectsDSConfig.use { implicit pcc =>
        val commonSameAs = datasetExternalSameAs.generateOne
        val (dataset1, project1) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(datasetEntities(provenanceImportedExternal(commonSameAs)))
          .generateOne
        val (dataset3, project3) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset2).generateOne

        for {
          _ <- provisionTestProjects(project1, project2, project3)

          expectedUsedIns = List(toDatasetProject(project1, dataset1),
                                 toDatasetProject(project2, dataset2),
                                 toDatasetProject(project3, dataset3)
                            ).sorted
          expectedDS1 = importedExternalToNonModified(dataset1, project1).copy(usedIn = expectedUsedIns)
          _ <- findById(dataset1.identifier).asserting(_.value shouldBe expectedDS1)

          expectedDS3 = importedInternalToNonModified(dataset3, project3).copy(
                          sameAs = dataset2.provenance.sameAs,
                          usedIn = expectedUsedIns
                        )
          _ <- findById(dataset3.identifier).asserting(_.value shouldBe expectedDS3)

          expectedDS2 = importedExternalToNonModified(dataset2, project2).copy(usedIn = expectedUsedIns)
          _ <- findBySameAs(commonSameAs)
                 .asserting(_.value should (be(expectedDS1) or be(expectedDS2) or be(expectedDS3)))
        } yield Succeeded
      }

    "return details of the dataset with the given id " +
      "- case where the first dataset is an Internal dataset" in projectsDSConfig.use { implicit pcc =>
        val (dataset1, project1) =
          anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceInternal)).generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset1).generateOne
        val (dataset3, project3) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset2).generateOne

        for {
          _ <- provisionTestProjects(project1, project2, project3)

          expectedUsedIns = List(toDatasetProject(project1, dataset1),
                                 toDatasetProject(project2, dataset2),
                                 toDatasetProject(project3, dataset3)
                            ).sorted
          expectedDS1 = internalToNonModified(dataset1, project1).copy(usedIn = expectedUsedIns)
          _ <- findById(dataset1.identifier).asserting(_.value shouldBe expectedDS1)

          expectedDS3 = importedInternalToNonModified(dataset3, project3).copy(
                          sameAs = SameAs(dataset1.provenance.entityId),
                          usedIn = expectedUsedIns
                        )
          _ <- findById(dataset3.identifier).asserting(_.value shouldBe expectedDS3)

          expectedDS2 = importedInternalToNonModified(dataset2, project2).copy(usedIn = expectedUsedIns)
          _ <- findByTopmostSameAs(dataset1.provenance.topmostSameAs)
                 .asserting(_.value should (be(expectedDS1) or be(expectedDS2) or be(expectedDS3)))
        } yield Succeeded
      }

    "return details of the dataset with the given id " +
      "- case where the sameAs hierarchy is broken by dataset modification" in projectsDSConfig.use { implicit pcc =>
        val (dataset1, project1) =
          anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceInternal)).generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPrivate).importDataset(dataset1).generateOne
        val (dataset2Modified, project2Updated0) = project2.addDataset(dataset2.createModification())
        val project2Updated                      = replaceVisibility(Visibility.Public).apply(project2Updated0)
        val (_, project3) = anyRenkuProjectEntities(visibilityPrivate).importDataset(dataset2Modified).generateOne

        for {
          _ <- provisionTestProjects(project1, project2Updated, project3)

          expectedDS1 = internalToNonModified(dataset1, project1)
          _ <- findById(dataset1.identifier).asserting(_.value shouldBe expectedDS1)
          _ <- findByTopmostSameAs(dataset1.provenance.topmostSameAs).asserting(_.value shouldBe expectedDS1)

          expectedDS2Modified = modifiedToModified(dataset2Modified, dataset1.provenance.date, project2Updated)
                                  .copy(usedIn = List(toDatasetProject(project2Updated, dataset2Modified)).sorted)
          _ <- findById(dataset2Modified.identifier).asserting(_.value shouldBe expectedDS2Modified)
          _ <- findByTopmostSameAs(dataset2Modified.provenance.topmostSameAs)
                 .asserting(_.value shouldBe expectedDS2Modified)
        } yield Succeeded
      }

    "not return details of a dataset" +
      "- case where the latest import of the dataset has been invalidated" in projectsDSConfig.use { implicit pcc =>
        val (dataset1, project1) =
          anyRenkuProjectEntities(visibilityPrivate).addDataset(datasetEntities(provenanceInternal)).generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset1).generateOne
        val dataset2Invalidation = dataset2.invalidateNow(personEntities)
        val project2Updated      = project2.addDatasets(dataset2Invalidation)

        for {
          _ <- provisionTestProjects(project1, project2Updated)

          _ <- findById(dataset2.identifier).asserting(_ shouldBe None)
          _ <- findByTopmostSameAs(dataset2.provenance.topmostSameAs).asserting(_ shouldBe None)
          _ <- findById(dataset2Invalidation.identifier).asserting(_ shouldBe None)
        } yield Succeeded
      }

    "not return details of a dataset" +
      "- case where the original dataset has been invalidated" in projectsDSConfig.use { implicit pcc =>
        val (dataset1, project1) =
          anyRenkuProjectEntities(visibilityPublic).addDataset(datasetEntities(provenanceInternal)).generateOne
        val (dataset2, project2) = anyRenkuProjectEntities(visibilityPublic).importDataset(dataset1).generateOne
        val dataset1Invalidation = dataset1.invalidateNow(personEntities)
        val project1Updated      = project1.addDatasets(dataset1Invalidation)

        for {
          _ <- provisionTestProjects(project1Updated, project2)

          _ <- findById(dataset1.identifier).asserting(_ shouldBe None)
          _ <- findById(dataset1Invalidation.identifier).asserting(_ shouldBe None)
          _ <- findByTopmostSameAs(dataset1.provenance.topmostSameAs).asserting(
                 _.map(_.slug) shouldBe Some(dataset2.identification.slug)
               )

          expectedDS2 = importedInternalToNonModified(dataset2, project2)
          _ <- findById(dataset2.identifier).asserting(_.value shouldBe expectedDS2)
          _ <- findByTopmostSameAs(dataset2.provenance.topmostSameAs).asserting(_.value shouldBe expectedDS2)
        } yield Succeeded
      }

    "not return details of a dataset" +
      "- case where the latest modification of the dataset has been invalidated" in projectsDSConfig.use {
        implicit pcc =>
          val (dataset -> datasetModified, project) = anyRenkuProjectEntities(visibilityPublic)
            .addDatasetAndModification(datasetEntities(provenanceInternal))
            .generateOne
          val datasetInvalidation = datasetModified.invalidateNow(personEntities)
          val projectUpdated      = project.addDatasets(datasetInvalidation)

          for {
            _ <- provisionTestProject(projectUpdated)

            _ <- findById(dataset.identifier)
                   .asserting(_.value shouldBe internalToNonModified(dataset, projectUpdated).copy(usedIn = Nil))
            _ <- findByTopmostSameAs(dataset.provenance.topmostSameAs).asserting(_ shouldBe None)

            _ <- findById(datasetModified.identifier).asserting(_ shouldBe None)
            _ <- findByTopmostSameAs(datasetModified.provenance.topmostSameAs).asserting(_ shouldBe None)

            _ <- findById(datasetInvalidation.identifier).asserting(_ shouldBe None)
            _ <- findByTopmostSameAs(datasetInvalidation.provenance.topmostSameAs).asserting(_ shouldBe None)
          } yield Succeeded
      }
  }

  "findDataset in the case the DS was imported from a tag" should {

    "return info about the initial tag if it was imported from a tag on another Renku DS " +
      "which means there are PublicationEvents on both original and imported DS with the same name and desc, " +
      "there's sameAs on the imported DS to the original DS " +
      "and imported DS has schema:version the same as the PublicationEvent name" in projectsDSConfig.use {
        implicit pcc =>
          val (originalDS, originalDSProject) = anyRenkuProjectEntities(visibilityPublic)
            .addDataset(
              datasetEntities(provenanceInternal).modify(_.replacePublicationEvents(List(publicationEventFactory)))
            )
            .generateOne

          val originalDSTag = originalDS.publicationEvents.head

          val (importedDS, importedDSProject) =
            anyRenkuProjectEntities(visibilityPublic).importDataset(originalDSTag).generateOne

          for {
            _ <- provisionTestProjects(originalDSProject, importedDSProject)

            expectedUsedIns = List(
                                toDatasetProject(originalDSProject, originalDS),
                                toDatasetProject(importedDSProject, importedDS)
                              ).sorted
            expectedImportedDS = importedInternalToNonModified(importedDS, importedDSProject).copy(
                                   maybeInitialTag = Tag(originalDSTag.name, originalDSTag.maybeDescription).some,
                                   usedIn = expectedUsedIns
                                 )
            _ <- findById(importedDS.identifier).asserting(_.value shouldBe expectedImportedDS)

            expectedOriginalDS = internalToNonModified(originalDS, originalDSProject).copy(usedIn = expectedUsedIns)
            _ <- findByTopmostSameAs(importedDS.provenance.topmostSameAs).asserting(
                   _.value should (be(expectedImportedDS) or be(expectedOriginalDS))
                 )
          } yield Succeeded
      }

    "not return info about the initial tag even if it was imported from a tag on another Renku DS " +
      "but the user has no access to the project of the original DS anymore" in projectsDSConfig.use { implicit pcc =>
        val (originalDS, originalDSProject) = anyRenkuProjectEntities(visibilityPrivate)
          .addDataset(
            datasetEntities(provenanceInternal).modify(_.replacePublicationEvents(List(publicationEventFactory)))
          )
          .generateOne

        val originalDSTag = originalDS.publicationEvents.head

        val (importedDS, importedDSProject) =
          anyRenkuProjectEntities(visibilityPublic).importDataset(originalDSTag).generateOne

        for {
          _ <- provisionTestProjects(originalDSProject, importedDSProject)

          expectedDS = importedInternalToNonModified(importedDS, importedDSProject)
          _ <- findById(importedDS.identifier).asserting(_.value shouldBe expectedDS)
          _ <- findByTopmostSameAs(importedDS.provenance.topmostSameAs).asserting(_.value shouldBe expectedDS)
        } yield Succeeded
      }

    "not return info about the initial tag if " +
      "there are PublicationEvents with the same name and desc on both original and imported DS " +
      "but the version on the imported DS is different that the event name" in projectsDSConfig.use { implicit pcc =>
        val (originalDS, originalDSProject) = anyRenkuProjectEntities(visibilityPublic)
          .addDataset(
            datasetEntities(provenanceInternal).modify(_.replacePublicationEvents(List(publicationEventFactory)))
          )
          .generateOne

        val originalDSTag = originalDS.publicationEvents.head

        val (importedDS, importedDSProject) = {
          val (ds, proj)    = anyRenkuProjectEntities(visibilityPublic).importDataset(originalDS).generateOne
          val dsWithSameTag = ds.copy(publicationEventFactories = List(originalDSTag.toFactory))
          dsWithSameTag -> proj.replaceDatasets(dsWithSameTag)
        }

        for {
          _ <- provisionTestProjects(originalDSProject, importedDSProject)

          _ = originalDSTag.name             shouldBe importedDS.publicationEvents.head.name
          _ = originalDSTag.maybeDescription shouldBe importedDS.publicationEvents.head.maybeDescription

          expectedImportedDS = importedInternalToNonModified(importedDS, importedDSProject).copy(
                                 maybeInitialTag = None,
                                 usedIn = List(toDatasetProject(originalDSProject, originalDS),
                                               toDatasetProject(importedDSProject, importedDS)
                                 ).sorted
                               )
          _ <- findById(importedDS.identifier).asserting(_.value shouldBe expectedImportedDS)

          expectedOriginalDS = internalToNonModified(originalDS, originalDSProject).copy(
                                 maybeInitialTag = None,
                                 usedIn = List(toDatasetProject(originalDSProject, originalDS),
                                               toDatasetProject(importedDSProject, importedDS)
                                 ).sorted
                               )
          _ <- findByTopmostSameAs(importedDS.provenance.topmostSameAs)
                 .asserting(_.value should (be(expectedImportedDS) or be(expectedOriginalDS)))
        } yield Succeeded
      }
  }

  implicit override val ioLogger:     TestLogger[IO] = TestLogger[IO]()
  private implicit lazy val renkuUrl: RenkuUrl       = renkuUrls.generateOne

  private def datasetFinder(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new DatasetFinderImpl[IO](
      new BaseDetailsFinderImpl[IO](pcc),
      new CreatorsFinderImpl[IO](pcc),
      new PartsFinderImpl[IO](pcc),
      new ProjectsFinderImpl[IO](pcc)
    )
  }

  private def findById(identifier: Identifier, authUser: AuthUser)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Option[details.Dataset]] =
    findDS(RequestedDataset(identifier), authUser)

  private def findById(identifier: Identifier)(implicit pcc: ProjectsConnectionConfig): IO[Option[details.Dataset]] =
    findDS(RequestedDataset(identifier))

  private def findBySameAs(sameAs: SameAs)(implicit pcc: ProjectsConnectionConfig): IO[Option[details.Dataset]] =
    findDS(RequestedDataset(sameAs))

  private def findByTopmostSameAs(topmostSameAs: TopmostSameAs)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Option[details.Dataset]] =
    findDS(RequestedDataset(SameAs.ofUnsafe(topmostSameAs.value)))

  private def findByTopmostSameAs(topmostSameAs: TopmostSameAs, authUser: AuthUser)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Option[details.Dataset]] =
    findDS(RequestedDataset(SameAs.ofUnsafe(topmostSameAs.value)), authUser)

  private def findDS(requestedDS: RequestedDataset)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Option[details.Dataset]] =
    findDS(requestedDS, AuthContext.forUnknownUser(requestedDS))

  private def findDS(requestedDS: RequestedDataset, authUser: AuthUser)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Option[details.Dataset]] =
    findDS(requestedDS, AuthContext(authUser.some, requestedDS))

  private def findDS(requestedDS: RequestedDataset, authContext: AuthContext[RequestedDataset])(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Option[details.Dataset]] =
    datasetFinder.findDataset(requestedDS, authContext)

  private implicit lazy val usedInOrdering: Ordering[DatasetProject] = Ordering.by(_.name)
}
