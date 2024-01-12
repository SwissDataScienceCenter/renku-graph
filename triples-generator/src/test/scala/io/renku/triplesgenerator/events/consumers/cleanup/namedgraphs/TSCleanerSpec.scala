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

package io.renku.triplesgenerator.events.consumers.cleanup.namedgraphs

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.SearchGraphsCleaner
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.countingGen
import io.renku.graph.model._
import io.renku.graph.model.datasets.TopmostSameAs
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.views.RdfResource
import io.renku.interpreters.TestLogger
import io.renku.jsonld.EntityId
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesgenerator.events.consumers.membersync.ProjectAuthSync
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class TSCleanerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with ScalaCheckPropertyChecks
    with EntitiesGenerators
    with should.Matchers
    with OptionValues
    with AsyncMockFactory {

  "removeTriples" should {

    forAll(renkuProjectEntitiesWithDatasetsAndActivities, countingGen) { (project, attempt) =>
      s"remove all activities, datasets and their dependant entities of the project #$attempt" in projectsDSConfig.use {
        implicit pcc =>
          for {
            _ <- uploadToProjects(project)

            _ = givenProjectIdFindingSucceeds(project)
            _ = givenSearchGraphsCleaningSucceeds(project)
            _ = givenProjectAuthSync(project)

            _ <- cleaner.removeTriples(project.slug)

            _ <- findAllData(project.resourceId).asserting(_ shouldBe List.empty)
          } yield Succeeded
      }
    }

    "remove project with all the entities " +
      "and remove the parent relationship on the child" in projectsDSConfig.use { implicit pcc =>
        val (parent, child) = renkuProjectEntitiesWithDatasetsAndActivities.generateOne
          .forkOnce()

        for {
          _ <- uploadToProjects(parent, child)

          _ <- findProjectParent(child.resourceId).asserting(_ shouldBe parent.resourceId.some)

          _ = givenProjectIdFindingSucceeds(parent)
          _ = givenSearchGraphsCleaningSucceeds(parent)
          _ = givenProjectAuthSync(parent)

          _ <- cleaner.removeTriples(parent.slug)

          _ <- findProject(parent.resourceId).asserting(_ shouldBe Nil)
          _ <- findProject(child.resourceId).asserting(_.isEmpty shouldBe false)
          _ <- findProjectParent(child.resourceId).asserting(_ shouldBe None)
        } yield Succeeded
      }

    "remove project with all the entities " +
      "and relink the parent relationship between children and grandparent" in projectsDSConfig.use { implicit pcc =>
        val grandparent ::~ (parent ::~ child) =
          renkuProjectEntitiesWithDatasetsAndActivities.generateOne
            .forkOnce()
            .map(_.forkOnce())

        for {
          _ <- uploadToProjects(grandparent, parent, child)

          _ <- findProjectParent(parent.resourceId).asserting(_ shouldBe grandparent.resourceId.some)
          _ <- findProjectParent(child.resourceId).asserting(_ shouldBe parent.resourceId.some)

          _ = givenProjectIdFindingSucceeds(parent)
          _ = givenSearchGraphsCleaningSucceeds(parent)
          _ = givenProjectAuthSync(parent)

          _ <- cleaner.removeTriples(parent.slug)

          _ <- findProject(parent.resourceId).asserting(_ shouldBe Nil)
          _ <- findProject(child.resourceId).asserting(_.isEmpty shouldBe false)
          _ <- findProjectParent(child.resourceId).asserting(_ shouldBe grandparent.resourceId.some)
        } yield Succeeded
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is the first in the sameAs hierarchy is removed " +
      "- external DS" in projectsDSConfig.use { implicit pcc =>
        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceImportedExternal)).generateOne

        val (middleProjectDS, middleProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        for {
          _ <- uploadToProjects(topProject, middleProject, bottomProject)

          _ = givenProjectIdFindingSucceeds(topProject)
          _ = givenSearchGraphsCleaningSucceeds(topProject)
          _ = givenProjectAuthSync(topProject)

          _ <- cleaner.removeTriples(topProject.slug)

          _ <- findProject(topProject.resourceId).asserting(_ shouldBe List.empty)
          _ <- findDataset(topProjectDS.entityId).asserting(_ shouldBe Nil)

          _ <- findSameAs(middleProjectDS.entityId).map(_.value).asserting { case (middleSameAs, middleTopmostSameAs) =>
                 middleSameAs              shouldBe topProjectDS.provenance.sameAs.some
                 middleTopmostSameAs.value shouldBe topProjectDS.provenance.sameAs.value
               }

          _ <- findSameAs(bottomProjectDS.entityId).map(_.value).asserting { case (bottomSameAs, bottomTopmostSameAs) =>
                 bottomSameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
                 bottomTopmostSameAs.value shouldBe topProjectDS.provenance.sameAs.value
               }
        } yield Succeeded
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is the first in the sameAs hierarchy is removed " +
      "- internal DS" in projectsDSConfig.use { implicit pcc =>
        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        for {
          _ <- uploadToProjects(middleProject, bottomProject, topProject)

          _ = givenProjectIdFindingSucceeds(topProject)
          _ = givenSearchGraphsCleaningSucceeds(topProject)
          _ = givenProjectAuthSync(topProject)

          _ <- cleaner.removeTriples(topProject.slug)

          _ <- findProject(topProject.resourceId).asserting(_ shouldBe List.empty)
          _ <- findDataset(topProjectDS.entityId).asserting(_ shouldBe Nil)

          _ <- findSameAs(middleProjectDS.entityId).map(_.value).asserting { case (middleSameAs, middleTopmostSameAs) =>
                 middleSameAs              shouldBe None
                 middleTopmostSameAs.value shouldBe middleProjectDS.entityId.value
               }

          _ <- findSameAs(bottomProjectDS.entityId).map(_.value).asserting { case (bottomSameAs, bottomTopmostSameAs) =>
                 bottomSameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
                 bottomTopmostSameAs.value shouldBe middleProjectDS.entityId.value
               }
        } yield Succeeded
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is in the middle of the sameAs hierarchy is removed" in projectsDSConfig.use {
        implicit pcc =>
          val (topProjectDS, topProject) =
            renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

          val (middleProjectDS, middleProject) =
            renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

          val (bottomProjectDS, bottomProject) =
            renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

          for {
            _ <- uploadToProjects(middleProject, bottomProject, topProject)

            _ = givenProjectIdFindingSucceeds(middleProject)
            _ = givenSearchGraphsCleaningSucceeds(middleProject)
            _ = givenProjectAuthSync(middleProject)

            _ <- cleaner.removeTriples(middleProject.slug)

            _ <- findProject(middleProject.resourceId).asserting(_ shouldBe List.empty)

            _ <- findSameAs(topProjectDS.entityId).map(_.value).asserting { case (topSameAs, topTopmostSameAs) =>
                   topSameAs              shouldBe None
                   topTopmostSameAs.value shouldBe topProjectDS.entityId.value
                 }

            _ <- findDataset(middleProjectDS.entityId).asserting(_ shouldBe Nil)

            _ <- findSameAs(bottomProjectDS.entityId)
                   .map(_.value)
                   .asserting { case (bottomSameAs, bottomTopmostSameAs) =>
                     bottomSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
                     bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
                   }
          } yield Succeeded
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is the last in the sameAs hierarchy is removed" in projectsDSConfig.use {
        implicit pcc =>
          val (topProjectDS, topProject) =
            renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

          val (middleProjectDS, middleProject) =
            renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

          val (bottomProjectDS, bottomProject) =
            renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

          for {
            _ <- uploadToProjects(middleProject, bottomProject, topProject)

            _ = givenProjectIdFindingSucceeds(bottomProject)
            _ = givenSearchGraphsCleaningSucceeds(bottomProject)
            _ = givenProjectAuthSync(bottomProject)

            _ <- cleaner.removeTriples(bottomProject.slug)

            _ <- findProject(bottomProject.resourceId).asserting(_ shouldBe List.empty)

            _ <- findSameAs(topProjectDS.entityId).map(_.value).asserting { case (topSameAs, topTopmostSameAs) =>
                   topSameAs              shouldBe None
                   topTopmostSameAs.value shouldBe topProjectDS.entityId.value
                 }

            _ <-
              findSameAs(middleProjectDS.entityId).map(_.value).asserting { case (middleSameAs, middleTopmostSameAs) =>
                middleSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
                middleTopmostSameAs.value shouldBe topProjectDS.entityId.value
              }

            _ <- findDataset(bottomProjectDS.entityId).asserting(_ shouldBe Nil)
          } yield Succeeded
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when there are multiple direct descendants of the DS on project to be removed" in projectsDSConfig.use {
        implicit pcc =>
          val (topProjectDS, topProject) =
            renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

          val (middleProject1DS, middleProject1) =
            renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

          val (middleProject2DS, middleProject2) =
            renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

          val (bottomProjectDS, bottomProject) =
            renkuProjectEntities(anyVisibility).importDataset(middleProject1DS).generateOne

          for {
            _ <- uploadToProjects(middleProject1, middleProject2, bottomProject, topProject)

            _ = givenProjectIdFindingSucceeds(topProject)
            _ = givenSearchGraphsCleaningSucceeds(topProject)
            _ = givenProjectAuthSync(topProject)

            _ <- cleaner.removeTriples(topProject.slug)

            _ <- findProject(topProject.resourceId).asserting(_ shouldBe List.empty)
            _ <- findDataset(topProjectDS.entityId).asserting(_ shouldBe Nil)

            _ <- findOutNewlyNominatedTopDS(middleProject1DS, middleProject2DS) >>= { newTopDS =>
                   if (newTopDS == middleProject1DS.entityId) {
                     for {
                       _ <- findSameAs(middleProject1DS.entityId).map(_.value).asserting {
                              case (newTopSameAs, newTopTopmostSameAs) =>
                                newTopSameAs              shouldBe None
                                newTopTopmostSameAs.value shouldBe middleProject1DS.entityId.value
                            }

                       _ <- findSameAs(middleProject2DS.entityId).map(_.value).asserting {
                              case (otherMiddleSameAs, otherMiddleTopmostSameAs) =>
                                otherMiddleSameAs.map(_.value) shouldBe middleProject1DS.entityId.value.some
                                otherMiddleTopmostSameAs.value shouldBe middleProject1DS.entityId.value
                            }

                       _ <- findSameAs(bottomProjectDS.entityId).map(_.value).asserting {
                              case (bottomSameAs, bottomTopmostSameAs) =>
                                bottomSameAs.map(_.value) shouldBe middleProject1DS.entityId.value.some
                                bottomTopmostSameAs.value shouldBe middleProject1DS.entityId.value
                            }
                     } yield ()
                   } else if (newTopDS == middleProject2DS.entityId) {
                     for {
                       _ <- findSameAs(middleProject2DS.entityId).map(_.value).asserting {
                              case (newTopSameAs, newTopTopmostSameAs) =>
                                newTopSameAs              shouldBe None
                                newTopTopmostSameAs.value shouldBe middleProject2DS.entityId.value
                            }

                       _ <- findSameAs(middleProject1DS.entityId).map(_.value).asserting {
                              case (otherMiddleSameAs, otherMiddleTopmostSameAs) =>
                                otherMiddleSameAs.map(_.value) shouldBe middleProject2DS.entityId.value.some
                                otherMiddleTopmostSameAs.value shouldBe middleProject2DS.entityId.value
                            }

                       _ <- findSameAs(bottomProjectDS.entityId).map(_.value).asserting {
                              case (bottomSameAs, bottomTopmostSameAs) =>
                                bottomSameAs.map(_.value) shouldBe middleProject1DS.entityId.value.some
                                bottomTopmostSameAs.value shouldBe middleProject2DS.entityId.value
                            }
                     } yield ()
                   } else fail("No descendant DS has been nominated as the new top DS")
                 }
          } yield Succeeded
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when there are multiple direct descendants of the DS on project to be removed " +
      "and some of the descendants have been modified" in projectsDSConfig.use { implicit pcc =>
        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProject1DS, middleProject1BeforeModif) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne
        val (middleProject1ModifDS, middleProject1) =
          middleProject1BeforeModif.addDataset(middleProject1DS.createModification())

        val (middleProject2DS, middleProject2) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProject1DS).generateOne

        for {
          _ <- uploadToProjects(topProject, middleProject1, middleProject2, bottomProject)

          _ = givenProjectIdFindingSucceeds(topProject)
          _ = givenSearchGraphsCleaningSucceeds(topProject)
          _ = givenProjectAuthSync(topProject)

          _ <- cleaner.removeTriples(topProject.slug)

          _ <- findProject(topProject.resourceId).asserting(_ shouldBe List.empty)
          _ <- findDataset(topProjectDS.entityId).asserting(_ shouldBe Nil)

          _ <- findSameAs(middleProject1DS.entityId).map(_.value).asserting {
                 case (middleProject1DSSameAs, middleProject1DSTopmostSameAs) =>
                   middleProject1DSSameAs.map(_.value) shouldBe middleProject2DS.entityId.value.some
                   middleProject1DSTopmostSameAs.value shouldBe middleProject2DS.entityId.value
               }

          _ <- findSameAs(middleProject1ModifDS.entityId).map(_.value).asserting {
                 case (middleProject1ModifDSSameAs, middleProject1ModifDSTopmostSameAs) =>
                   middleProject1ModifDSSameAs              shouldBe None
                   middleProject1ModifDSTopmostSameAs.value shouldBe middleProject1ModifDS.entityId.value
               }

          _ <-
            findSameAs(middleProject2DS.entityId).map(_.value).asserting { case (newTopSameAs, newTopTopmostSameAs) =>
              newTopSameAs              shouldBe None
              newTopTopmostSameAs.value shouldBe middleProject2DS.entityId.value
            }

          _ <- findSameAs(bottomProjectDS.entityId).map(_.value).asserting { case (bottomSameAs, bottomTopmostSameAs) =>
                 // It's a question if this SameAs should be middleProject2DS too.
                 // However, leaving things this way preserves the 'real' import hierarchy and middleProject1DS points to the new top DS anyway.
                 bottomSameAs.map(_.value) shouldBe middleProject1DS.entityId.value.some
                 bottomTopmostSameAs.value shouldBe middleProject2DS.entityId.value
               }
        } yield Succeeded
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a fork and containing a DS which is the top in the sameAs hierarchy is removed" in projectsDSConfig
        .use { implicit pcc =>
          val (topProjectDS, topProjectBeforeForking) =
            renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

          val (topProject, topProjectFork) = topProjectBeforeForking.forkOnce()

          val (bottomProjectDS, bottomProject) =
            renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

          for {
            _ <- uploadToProjects(bottomProject, topProject, topProjectFork)

            _ = givenProjectIdFindingSucceeds(topProject)
            _ = givenSearchGraphsCleaningSucceeds(topProject)
            _ = givenProjectAuthSync(topProject)

            _ <- cleaner.removeTriples(topProject.slug)

            _ <- findProject(topProject.resourceId).asserting(_ shouldBe List.empty)
            _ <- findProject(topProjectFork.resourceId).asserting(_.size should be > 0)

            _ <- findSameAs(topProjectDS.entityId).map(_.value).asserting { case (topSameAs, topTopmostSameAs) =>
                   topSameAs              shouldBe None
                   topTopmostSameAs.value shouldBe topProjectDS.entityId.value
                 }

            _ <-
              findSameAs(bottomProjectDS.entityId).map(_.value).asserting { case (bottomSameAs, bottomTopmostSameAs) =>
                bottomSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
                bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
              }
          } yield Succeeded
        }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a fork and containing a DS which is the middle in the sameAs hierarchy is removed" in projectsDSConfig
        .use { implicit pcc =>
          val (topProjectDS, topProject) =
            renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

          val (middleProjectDS, middleProjectBeforeForking) =
            renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

          val (middleProject, middleProjectFork) = middleProjectBeforeForking.forkOnce()

          val (bottomProjectDS, bottomProject) =
            renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

          for {
            _ <- uploadToProjects(topProject, middleProject, middleProjectFork, bottomProject)

            _ = givenProjectIdFindingSucceeds(middleProject)
            _ = givenSearchGraphsCleaningSucceeds(middleProject)
            _ = givenProjectAuthSync(middleProject)

            _ <- cleaner.removeTriples(middleProject.slug)

            _ <- findSameAs(topProjectDS.entityId).map(_.value).asserting { case (topSameAs, topTopmostSameAs) =>
                   topSameAs              shouldBe None
                   topTopmostSameAs.value shouldBe topProjectDS.entityId.value
                 }

            _ <- findProject(middleProject.resourceId).asserting(_ shouldBe List.empty)
            _ <- findProject(middleProjectFork.resourceId).asserting(_.size should be > 0)

            _ <-
              findSameAs(middleProjectDS.entityId).map(_.value).asserting { case (middleSameAs, middleTopmostSameAs) =>
                middleSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
                middleTopmostSameAs.value shouldBe topProjectDS.entityId.value
              }

            _ <-
              findSameAs(bottomProjectDS.entityId).map(_.value).asserting { case (bottomSameAs, bottomTopmostSameAs) =>
                bottomSameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
                bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
              }
          } yield Succeeded
        }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when both the project and its fork get deleted" in projectsDSConfig.use { implicit pcc =>
        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProjectBeforeForking) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (middleProject, middleProjectFork) = middleProjectBeforeForking.forkOnce()

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        for {
          _ <- uploadToProjects(topProject, middleProject, middleProjectFork, bottomProject)

          _ = givenProjectIdFindingSucceeds(middleProject)
          _ = givenProjectIdFindingSucceeds(middleProjectFork)
          _ = givenProjectAuthSync(middleProject)
          _ = givenSearchGraphsCleaningSucceeds(middleProject)
          _ = givenSearchGraphsCleaningSucceeds(middleProjectFork)
          _ = givenProjectAuthSync(middleProjectFork)

          _ <- cleaner.removeTriples(middleProject.slug)
          _ <- cleaner.removeTriples(middleProjectFork.slug)

          _ <- findSameAs(topProjectDS.entityId).map(_.value).asserting { case (topSameAs, topTopmostSameAs) =>
                 topSameAs              shouldBe None
                 topTopmostSameAs.value shouldBe topProjectDS.entityId.value
               }

          _ <- findProject(middleProject.resourceId).asserting(_ shouldBe List.empty)
          _ <- findProject(middleProjectFork.resourceId).asserting(_ shouldBe List.empty)
          _ <- findSameAs(middleProjectDS.entityId).asserting(_ shouldBe None)

          _ <- findSameAs(bottomProjectDS.entityId).map(_.value).asserting { case (bottomSameAs, bottomTopmostSameAs) =>
                 bottomSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
                 bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
               }
        } yield Succeeded
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a fork and a DS with multiple direct descendants is removed" in projectsDSConfig.use {
        implicit pcc =>
          val (topProjectDS, topProject) =
            renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

          val (middleProjectDS, middleProjectBeforeForking) =
            renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

          val (middleProject, middleProjectFork) = middleProjectBeforeForking.forkOnce()

          val (bottomProject1DS, bottomProject1) =
            renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

          val (bottomProject2DS, bottomProject2) =
            renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

          for {
            _ <- uploadToProjects(topProject, middleProject, middleProjectFork, bottomProject1, bottomProject2)

            _ = givenProjectIdFindingSucceeds(middleProject)
            _ = givenSearchGraphsCleaningSucceeds(middleProject)
            _ = givenProjectAuthSync(middleProject)

            _ <- cleaner.removeTriples(middleProject.slug)

            _ <- findSameAs(topProjectDS.entityId).map(_.value).asserting { case (topSameAs, topTopmostSameAs) =>
                   topSameAs              shouldBe None
                   topTopmostSameAs.value shouldBe topProjectDS.entityId.value
                 }

            _ <- findProject(middleProject.resourceId).asserting(_ shouldBe List.empty)
            _ <- findProject(middleProjectFork.resourceId).asserting(_.size should be > 0)

            _ <-
              findSameAs(middleProjectDS.entityId).map(_.value).asserting { case (middleSameAs, middleTopmostSameAs) =>
                middleSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
                middleTopmostSameAs.value shouldBe topProjectDS.entityId.value
              }

            _ <- findSameAs(bottomProject1DS.entityId).map(_.value).asserting {
                   case (bottom1SameAs, bottom1TopmostSameAs) =>
                     bottom1SameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
                     bottom1TopmostSameAs.value shouldBe topProjectDS.entityId.value
                 }

            _ <- findSameAs(bottomProject2DS.entityId).map(_.value).asserting {
                   case (bottom2SameAs, bottom2TopmostSameAs) =>
                     bottom2SameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
                     bottom2TopmostSameAs.value shouldBe topProjectDS.entityId.value
                 }
          } yield Succeeded
      }
  }

  private def findAllData(projectId: projects.ResourceId)(implicit pcc: ProjectsConnectionConfig) =
    runSelect(
      SparqlQuery.of(
        "find all data",
        s"""|SELECT ?s ?p ?o
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> { ?s ?p ?o }
            |}""".stripMargin
      )
    ).map(_.map(_.values.toList))

  private def findProject(projectId: projects.ResourceId)(implicit pcc: ProjectsConnectionConfig) =
    runSelect(
      SparqlQuery.of(
        "find project triples",
        s"""SELECT ?p ?o
          WHERE {
            GRAPH <${GraphClass.Project.id(projectId)}> {
              ${projectId.showAs[RdfResource]} ?p ?o
            }
          }"""
      )
    ).map(_.map(_.values.toList))

  private def findProjectParent(projectId: projects.ResourceId)(implicit pcc: ProjectsConnectionConfig) =
    runSelect(
      SparqlQuery.of(
        "find project parent",
        Prefixes of prov -> "prov",
        s"""SELECT ?parentId
          WHERE {
            GRAPH <${GraphClass.Project.id(projectId)}> {
              ${projectId.showAs[RdfResource]} prov:wasDerivedFrom ?parentId
            }
          }"""
      )
    ).map(_.map(row => row.get("parentId").map(projects.ResourceId))).map {
      case Nil                => None
      case maybeParent :: Nil => maybeParent
      case _                  => fail(s"Multiple wasDerivedFrom for $projectId")
    }

  private def findDataset(datasetId: EntityId)(implicit pcc: ProjectsConnectionConfig) =
    runSelect(
      SparqlQuery.of(
        "find DS triples",
        s"""SELECT ?p ?o
          WHERE {
            GRAPH ?g { <$datasetId> ?p ?o }
          }"""
      )
    ).map(_.map(_.values.toList))

  private def findSameAs(datasetId: EntityId)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Option[(Option[datasets.SameAs], TopmostSameAs)]] =
    runSelect(
      SparqlQuery.of(
        "find DS sameAs",
        Prefixes.of(renku -> "renku", schema -> "schema"),
        s"""SELECT ?sameAs ?topmostSameAs
          WHERE {
            GRAPH ?g {
              <$datasetId> renku:topmostSameAs ?topmostSameAs .
              OPTIONAL {<$datasetId> schema:sameAs/schema:url ?sameAs }
            }
          }"""
      )
    ) map {
      case Nil => None
      case props :: Nil =>
        Some(props.get("sameAs").map(datasets.SameAs) -> datasets.TopmostSameAs(props("topmostSameAs")))
      case _ => fail(s"There multiple (SameAs, TopmostSameAs) tuples for the DS $datasetId")
    }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private val projectIdFinder      = mock[ProjectIdFinder[IO]]
  private val searchGraphsCleaner  = mock[SearchGraphsCleaner[IO]]
  private lazy val projectAuthSync = mock[ProjectAuthSync[IO]]
  private def cleaner(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new TSCleanerImpl[IO](projectIdFinder, searchGraphsCleaner, projectAuthSync, pcc)
  }

  private def givenProjectIdFindingSucceeds(project: Project) =
    (projectIdFinder.findProjectId _)
      .expects(project.slug)
      .returning(project.identification.some.pure[IO])

  private def givenSearchGraphsCleaningSucceeds(project: Project) =
    (searchGraphsCleaner.cleanSearchGraphs _)
      .expects(project.identification)
      .returning(().pure[IO])

  private def givenProjectAuthSync(project: Project) =
    (projectAuthSync.removeAuthData _)
      .expects(project.slug)
      .returning(IO.unit)

  private def findOutNewlyNominatedTopDS(ds1: Dataset[Dataset.Provenance], ds2: Dataset[Dataset.Provenance])(implicit
      pcc: ProjectsConnectionConfig
  ): IO[EntityId] =
    findSameAs(ds1.entityId).map {
      case Some(None -> _) => ds1.entityId
      case Some(Some(_) -> _) =>
        findSameAs(ds2.entityId).unsafeRunSync() match {
          case Some(None -> _) => ds2.entityId
          case _               => fail("No descendant DS has been nominated")
        }
      case None => fail("No descendant DS has been nominated")
    }
}
