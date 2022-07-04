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

package io.renku.triplesgenerator.events.consumers.cleanup

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.graph.model.datasets.TopmostSameAs
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{datasets, projects}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax.JsonEncoderOps
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectTriplesRemoverSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryRdfStore
    with ScalaCheckPropertyChecks
    with EntitiesGenerators {

  "removeTriples" should {

    "remove all activities, datasets and their dependant entities of a project and the project itself" in new TestCase {
      forAll(renkuProjectEntitiesWithDatasetsAndActivities) { project =>
        loadToStore(project.asJsonLD)

        projectTriplesRemover.removeTriples(project.path).unsafeRunSync()

        findAllData.unsafeRunSync() shouldBe List.empty
      }
    }

    "remove all entities which are linked only to the current project " +
      "and the project itself" in new TestCase {
        forAll(renkuProjectEntitiesWithDatasetsAndActivities) { project =>
          val (projectData, child) = project.forkOnce()

          loadToStore(projectData.asJsonLD, child.asJsonLD)

          projectTriplesRemover.removeTriples(projectData.path).unsafeRunSync()

          findProject(projectData.path).unsafeRunSync()   shouldBe List.empty
          findProject(child.path).unsafeRunSync().isEmpty shouldBe false
        }
      }

    "remove the project and all entities which are linked to it " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is the first in the sameAs hierarchy is removed " +
      "while it's an imported DS" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceImportedExternal)).generateOne

        val (middleProjectDS, middleProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        loadToStore(middleProject, bottomProject, topProject)

        projectTriplesRemover.removeTriples(topProject.path).unsafeRunSync()

        findProject(topProject.path).unsafeRunSync()       shouldBe List.empty
        findDataset(topProjectDS.entityId).unsafeRunSync() shouldBe Nil

        val Some((middleSameAs, middleTopmostSameAs)) = findSameAs(middleProjectDS.entityId).unsafeRunSync()
        middleSameAs              shouldBe topProjectDS.provenance.sameAs.some
        middleTopmostSameAs.value shouldBe topProjectDS.provenance.sameAs.value

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe topProjectDS.provenance.sameAs.value
      }

    "remove the project and all entities which are linked to it " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is the first in the sameAs hierarchy is removed" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        loadToStore(middleProject, bottomProject, topProject)

        projectTriplesRemover.removeTriples(topProject.path).unsafeRunSync()

        findProject(topProject.path).unsafeRunSync()       shouldBe List.empty
        findDataset(topProjectDS.entityId).unsafeRunSync() shouldBe Nil

        val Some((middleSameAs, middleTopmostSameAs)) = findSameAs(middleProjectDS.entityId).unsafeRunSync()
        middleSameAs              shouldBe None
        middleTopmostSameAs.value shouldBe middleProjectDS.entityId.value

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe middleProjectDS.entityId.value
      }

    "remove the project and all entities which are linked to it " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is in the middle of the sameAs hierarchy is removed" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        loadToStore(middleProject, bottomProject, topProject)

        projectTriplesRemover.removeTriples(middleProject.path).unsafeRunSync()

        findProject(middleProject.path).unsafeRunSync() shouldBe List.empty

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        findDataset(middleProjectDS.entityId).unsafeRunSync() shouldBe Nil

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
      }

    "remove the project and all entities which are linked to it " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is the last in the sameAs hierarchy is removed" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        loadToStore(middleProject, bottomProject, topProject)

        projectTriplesRemover.removeTriples(bottomProject.path).unsafeRunSync()

        findProject(bottomProject.path).unsafeRunSync() shouldBe List.empty

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        val Some((middleSameAs, middleTopmostSameAs)) = findSameAs(middleProjectDS.entityId).unsafeRunSync()
        middleSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
        middleTopmostSameAs.value shouldBe topProjectDS.entityId.value

        findDataset(bottomProjectDS.entityId).unsafeRunSync() shouldBe Nil
      }

    "remove the project and all entities which are linked to it " +
      "and update the sameAs hierarchy " +
      "when there are multiple direct descendants of the DS on project to be removed" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProject1DS, middleProject1) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (middleProject2DS, middleProject2) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProject1DS).generateOne

        loadToStore(middleProject1, middleProject2, bottomProject, topProject)

        projectTriplesRemover.removeTriples(topProject.path).unsafeRunSync()

        findProject(topProject.path).unsafeRunSync()       shouldBe List.empty
        findDataset(topProjectDS.entityId).unsafeRunSync() shouldBe Nil

        val newTopDS = findOutNewlyNominatedTopDS(middleProject1DS, middleProject2DS)

        if (newTopDS == middleProject1DS.entityId) {
          val Some((newTopSameAs, newTopTopmostSameAs)) = findSameAs(middleProject1DS.entityId).unsafeRunSync()
          newTopSameAs              shouldBe None
          newTopTopmostSameAs.value shouldBe middleProject1DS.entityId.value

          val Some((otherMiddleSameAs, otherMiddleTopmostSameAs)) =
            findSameAs(middleProject2DS.entityId).unsafeRunSync()
          otherMiddleSameAs.map(_.value) shouldBe middleProject1DS.entityId.value.some
          otherMiddleTopmostSameAs.value shouldBe middleProject1DS.entityId.value

          val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
          bottomSameAs.map(_.value) shouldBe middleProject1DS.entityId.value.some
          bottomTopmostSameAs.value shouldBe middleProject1DS.entityId.value
        } else if (newTopDS == middleProject2DS.entityId) {
          val Some((newTopSameAs, newTopTopmostSameAs)) = findSameAs(middleProject2DS.entityId).unsafeRunSync()
          newTopSameAs              shouldBe None
          newTopTopmostSameAs.value shouldBe middleProject2DS.entityId.value

          val Some((otherMiddleSameAs, otherMiddleTopmostSameAs)) =
            findSameAs(middleProject1DS.entityId).unsafeRunSync()
          otherMiddleSameAs.map(_.value) shouldBe middleProject2DS.entityId.value.some
          otherMiddleTopmostSameAs.value shouldBe middleProject2DS.entityId.value

          val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
          bottomSameAs.map(_.value) shouldBe middleProject1DS.entityId.value.some
          bottomTopmostSameAs.value shouldBe middleProject2DS.entityId.value
        } else fail("No descendant DS has been nominated as the new top DS")
      }

    "remove the project and all entities which are linked to it " +
      "and update the sameAs hierarchy " +
      "when there are multiple direct descendants of the DS on project to be removed " +
      "and some of the descendants have been modified" in new TestCase {

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

        loadToStore(topProject, middleProject1, middleProject2, bottomProject)

        projectTriplesRemover.removeTriples(topProject.path).unsafeRunSync()

        findProject(topProject.path).unsafeRunSync()       shouldBe List.empty
        findDataset(topProjectDS.entityId).unsafeRunSync() shouldBe Nil

        val Some((middleProject1DSSameAs, middleProject1DSTopmostSameAs)) =
          findSameAs(middleProject1DS.entityId).unsafeRunSync()
        middleProject1DSSameAs.map(_.value) shouldBe middleProject2DS.entityId.value.some
        middleProject1DSTopmostSameAs.value shouldBe middleProject2DS.entityId.value

        val Some((middleProject1ModifDSSameAs, middleProject1ModifDSTopmostSameAs)) =
          findSameAs(middleProject1ModifDS.entityId).unsafeRunSync()
        middleProject1ModifDSSameAs              shouldBe None
        middleProject1ModifDSTopmostSameAs.value shouldBe middleProject1ModifDS.entityId.value

        val Some((newTopSameAs, newTopTopmostSameAs)) = findSameAs(middleProject2DS.entityId).unsafeRunSync()
        newTopSameAs              shouldBe None
        newTopTopmostSameAs.value shouldBe middleProject2DS.entityId.value

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        // It's a question if this SameAs should be middleProject2DS too.
        // However, leaving things this way preserves the 'real' import hierarchy and middleProject1DS points to the new top DS anyway.
        bottomSameAs.map(_.value) shouldBe middleProject1DS.entityId.value.some
        bottomTopmostSameAs.value shouldBe middleProject2DS.entityId.value
      }

    "remove the project and all entities which are linked to it " +
      "and update the sameAs hierarchy " +
      "when project containing a fork and containing a DS which is the top in the sameAs hierarchy is removed" in new TestCase {

        val (topProjectDS, topProjectBeforeForking) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (topProject, topProjectFork) = topProjectBeforeForking.forkOnce()

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        loadToStore(bottomProject.asJsonLD, topProject.asJsonLD, topProjectFork.asJsonLD)

        projectTriplesRemover.removeTriples(topProject.path).unsafeRunSync()

        findProject(topProject.path).unsafeRunSync()        shouldBe List.empty
        findProject(topProjectFork.path).unsafeRunSync().size should be > 0

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
      }

    "remove the project and all entities which are linked to it " +
      "and update the sameAs hierarchy " +
      "when project containing a fork and containing a DS which is the middle in the sameAs hierarchy is removed" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProjectBeforeForking) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (middleProject, middleProjectFork) = middleProjectBeforeForking.forkOnce()

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        loadToStore(topProject.asJsonLD, middleProject.asJsonLD, middleProjectFork.asJsonLD, bottomProject.asJsonLD)

        projectTriplesRemover.removeTriples(middleProject.path).unsafeRunSync()

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        findProject(middleProject.path).unsafeRunSync()        shouldBe List.empty
        findProject(middleProjectFork.path).unsafeRunSync().size should be > 0

        val Some((middleSameAs, middleTopmostSameAs)) = findSameAs(middleProjectDS.entityId).unsafeRunSync()
        middleSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
        middleTopmostSameAs.value shouldBe topProjectDS.entityId.value

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
      }

    "remove the project and all entities which are linked to it " +
      "and update the sameAs hierarchy " +
      "when both the project and its fork get deleted" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProjectBeforeForking) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (middleProject, middleProjectFork) = middleProjectBeforeForking.forkOnce()

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        loadToStore(topProject.asJsonLD, middleProject.asJsonLD, middleProjectFork.asJsonLD, bottomProject.asJsonLD)

        projectTriplesRemover.removeTriples(middleProject.path).unsafeRunSync()
        projectTriplesRemover.removeTriples(middleProjectFork.path).unsafeRunSync()

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        findProject(middleProject.path).unsafeRunSync()      shouldBe List.empty
        findProject(middleProjectFork.path).unsafeRunSync()  shouldBe List.empty
        findSameAs(middleProjectDS.entityId).unsafeRunSync() shouldBe None

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
      }

    "remove the project and all entities which are linked to it " +
      "and update the sameAs hierarchy " +
      "when project containing a fork and a DS with multiple direct descendants is removed" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProjectBeforeForking) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (middleProject, middleProjectFork) = middleProjectBeforeForking.forkOnce()

        val (bottomProject1DS, bottomProject1) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        val (bottomProject2DS, bottomProject2) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        loadToStore(topProject, middleProject, middleProjectFork, bottomProject1, bottomProject2)

        projectTriplesRemover.removeTriples(middleProject.path).unsafeRunSync()

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        findProject(middleProject.path).unsafeRunSync()        shouldBe List.empty
        findProject(middleProjectFork.path).unsafeRunSync().size should be > 0

        val Some((middleSameAs, middleTopmostSameAs)) = findSameAs(middleProjectDS.entityId).unsafeRunSync()
        middleSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
        middleTopmostSameAs.value shouldBe topProjectDS.entityId.value

        val Some((bottom1SameAs, bottom1TopmostSameAs)) = findSameAs(bottomProject1DS.entityId).unsafeRunSync()
        bottom1SameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
        bottom1TopmostSameAs.value shouldBe topProjectDS.entityId.value

        val Some((bottom2SameAs, bottom2TopmostSameAs)) = findSameAs(bottomProject2DS.entityId).unsafeRunSync()
        bottom2SameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
        bottom2TopmostSameAs.value shouldBe topProjectDS.entityId.value
      }
  }

  private def findAllData = runQuery(s"""SELECT ?s ?p ?o WHERE { ?s ?p ?o }""").map(_.map(_.values.toList))

  private def findProject(projectPath: projects.Path) = runQuery(
    s"""SELECT ?p ?o 
        WHERE { 
          <$renkuUrl/projects/$projectPath> ?p ?o .
        } """
  ).map(_.map(_.values.toList))

  private def findDataset(datasetId: EntityId) = runQuery(
    s"""SELECT ?p ?o 
        WHERE { 
          <$datasetId> ?p ?o .
        } """
  ).map(_.map(_.values.toList))

  private def findSameAs(datasetId: EntityId): IO[Option[(Option[datasets.SameAs], TopmostSameAs)]] = runQuery(
    s"""SELECT ?sameAs ?topmostSameAs 
        WHERE {
          <$datasetId> renku:topmostSameAs ?topmostSameAs . 
          OPTIONAL {<$datasetId> schema:sameAs/schema:url ?sameAs .}                                  
        }
       """
  ) map {
    case Nil => None
    case props :: Nil =>
      Some(props.get("sameAs").map(datasets.SameAs) -> datasets.TopmostSameAs(props("topmostSameAs")))
    case _ => fail(s"There multiple (SameAs, TopmostSameAs) tuples for the DS $datasetId")
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val projectTriplesRemover = new ProjectTriplesRemoverImpl[IO](renkuStoreConfig, renkuUrl)
  }

  private def findOutNewlyNominatedTopDS(ds1: Dataset[Dataset.Provenance], ds2: Dataset[Dataset.Provenance]): EntityId =
    findSameAs(ds1.entityId).unsafeRunSync() match {
      case Some(None -> _) => ds1.entityId
      case Some(Some(_) -> _) =>
        findSameAs(ds2.entityId).unsafeRunSync() match {
          case Some(None -> _) => ds2.entityId
          case _               => fail("No descendant DS has been nominated")
        }
      case None => fail("No descendant DS has been nominated")
    }
}
