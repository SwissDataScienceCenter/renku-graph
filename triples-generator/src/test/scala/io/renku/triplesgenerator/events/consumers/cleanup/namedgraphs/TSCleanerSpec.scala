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

package io.renku.triplesgenerator.events.consumers.cleanup.namedgraphs

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.SearchGraphsCleaner
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.datasets.TopmostSameAs
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.views.RdfResource
import io.renku.interpreters.TestLogger
import io.renku.jsonld.EntityId
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class TSCleanerSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with ScalaCheckPropertyChecks
    with EntitiesGenerators
    with MockFactory {

  "removeTriples" should {

    "remove all activities, datasets and their dependant entities of the project" in new TestCase {
      forAll(renkuProjectEntitiesWithDatasetsAndActivities) { project =>
        upload(to = projectsDataset, project)

        givenProjectIdFindingSucceeds(project)
        givenSearchGraphsCleaningSucceeds(project)

        (cleaner removeTriples project.path).unsafeRunSync()

        findAllData(project.resourceId).unsafeRunSync() shouldBe List.empty
      }
    }

    "remove project with all the entities " +
      "and remove the parent relationship on the child" in new TestCase {
        val (parent, child) = renkuProjectEntitiesWithDatasetsAndActivities.generateOne
          .forkOnce()

        upload(to = projectsDataset, parent, child)

        findProjectParent(child.resourceId).unsafeRunSync() shouldBe parent.resourceId.some

        givenProjectIdFindingSucceeds(parent)
        givenSearchGraphsCleaningSucceeds(parent)

        (cleaner removeTriples parent.path).unsafeRunSync()

        findProject(parent.resourceId).unsafeRunSync()        shouldBe Nil
        findProject(child.resourceId).unsafeRunSync().isEmpty shouldBe false
        findProjectParent(child.resourceId).unsafeRunSync()   shouldBe None
      }

    "remove project with all the entities " +
      "and relink the parent relationship between children and grandparent" in new TestCase {
        val grandparent ::~ (parent ::~ child) =
          renkuProjectEntitiesWithDatasetsAndActivities.generateOne
            .forkOnce()
            .map(_.forkOnce())

        upload(to = projectsDataset, grandparent, parent, child)

        findProjectParent(parent.resourceId).unsafeRunSync() shouldBe grandparent.resourceId.some
        findProjectParent(child.resourceId).unsafeRunSync()  shouldBe parent.resourceId.some

        givenProjectIdFindingSucceeds(parent)
        givenSearchGraphsCleaningSucceeds(parent)

        (cleaner removeTriples parent.path).unsafeRunSync()

        findProject(parent.resourceId).unsafeRunSync()        shouldBe Nil
        findProject(child.resourceId).unsafeRunSync().isEmpty shouldBe false
        findProjectParent(child.resourceId).unsafeRunSync()   shouldBe grandparent.resourceId.some
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is the first in the sameAs hierarchy is removed " +
      "- external DS" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceImportedExternal)).generateOne

        val (middleProjectDS, middleProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        upload(to = projectsDataset, topProject, middleProject, bottomProject)

        givenProjectIdFindingSucceeds(topProject)
        givenSearchGraphsCleaningSucceeds(topProject)

        (cleaner removeTriples topProject.path).unsafeRunSync()

        findProject(topProject.resourceId).unsafeRunSync() shouldBe List.empty
        findDataset(topProjectDS.entityId).unsafeRunSync() shouldBe Nil

        val Some((middleSameAs, middleTopmostSameAs)) = findSameAs(middleProjectDS.entityId).unsafeRunSync()
        middleSameAs              shouldBe topProjectDS.provenance.sameAs.some
        middleTopmostSameAs.value shouldBe topProjectDS.provenance.sameAs.value

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe topProjectDS.provenance.sameAs.value
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is the first in the sameAs hierarchy is removed " +
      "- internal DS" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        upload(to = projectsDataset, middleProject, bottomProject, topProject)

        givenProjectIdFindingSucceeds(topProject)
        givenSearchGraphsCleaningSucceeds(topProject)

        (cleaner removeTriples topProject.path).unsafeRunSync()

        findProject(topProject.resourceId).unsafeRunSync() shouldBe List.empty
        findDataset(topProjectDS.entityId).unsafeRunSync() shouldBe Nil

        val Some((middleSameAs, middleTopmostSameAs)) = findSameAs(middleProjectDS.entityId).unsafeRunSync()
        middleSameAs              shouldBe None
        middleTopmostSameAs.value shouldBe middleProjectDS.entityId.value

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe middleProjectDS.entityId.value
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is in the middle of the sameAs hierarchy is removed" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        upload(to = projectsDataset, middleProject, bottomProject, topProject)

        givenProjectIdFindingSucceeds(middleProject)
        givenSearchGraphsCleaningSucceeds(middleProject)

        (cleaner removeTriples middleProject.path).unsafeRunSync()

        findProject(middleProject.resourceId).unsafeRunSync() shouldBe List.empty

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        findDataset(middleProjectDS.entityId).unsafeRunSync() shouldBe Nil

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a DS which is the last in the sameAs hierarchy is removed" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        upload(to = projectsDataset, middleProject, bottomProject, topProject)

        givenProjectIdFindingSucceeds(bottomProject)
        givenSearchGraphsCleaningSucceeds(bottomProject)

        (cleaner removeTriples bottomProject.path).unsafeRunSync()

        findProject(bottomProject.resourceId).unsafeRunSync() shouldBe List.empty

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        val Some((middleSameAs, middleTopmostSameAs)) = findSameAs(middleProjectDS.entityId).unsafeRunSync()
        middleSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
        middleTopmostSameAs.value shouldBe topProjectDS.entityId.value

        findDataset(bottomProjectDS.entityId).unsafeRunSync() shouldBe Nil
      }

    "remove project with all the entities " +
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

        upload(to = projectsDataset, middleProject1, middleProject2, bottomProject, topProject)

        givenProjectIdFindingSucceeds(topProject)
        givenSearchGraphsCleaningSucceeds(topProject)

        (cleaner removeTriples topProject.path).unsafeRunSync()

        findProject(topProject.resourceId).unsafeRunSync() shouldBe List.empty
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

    "remove project with all the entities " +
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

        upload(to = projectsDataset, topProject, middleProject1, middleProject2, bottomProject)

        givenProjectIdFindingSucceeds(topProject)
        givenSearchGraphsCleaningSucceeds(topProject)

        (cleaner removeTriples topProject.path).unsafeRunSync()

        findProject(topProject.resourceId).unsafeRunSync() shouldBe List.empty
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

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a fork and containing a DS which is the top in the sameAs hierarchy is removed" in new TestCase {

        val (topProjectDS, topProjectBeforeForking) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (topProject, topProjectFork) = topProjectBeforeForking.forkOnce()

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        upload(to = projectsDataset, bottomProject, topProject, topProjectFork)

        givenProjectIdFindingSucceeds(topProject)
        givenSearchGraphsCleaningSucceeds(topProject)

        (cleaner removeTriples topProject.path).unsafeRunSync()

        findProject(topProject.resourceId).unsafeRunSync()        shouldBe List.empty
        findProject(topProjectFork.resourceId).unsafeRunSync().size should be > 0

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when project containing a fork and containing a DS which is the middle in the sameAs hierarchy is removed" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProjectBeforeForking) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (middleProject, middleProjectFork) = middleProjectBeforeForking.forkOnce()

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        upload(to = projectsDataset, topProject, middleProject, middleProjectFork, bottomProject)

        givenProjectIdFindingSucceeds(middleProject)
        givenSearchGraphsCleaningSucceeds(middleProject)

        cleaner.removeTriples(middleProject.path).unsafeRunSync()

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        findProject(middleProject.resourceId).unsafeRunSync()        shouldBe List.empty
        findProject(middleProjectFork.resourceId).unsafeRunSync().size should be > 0

        val Some((middleSameAs, middleTopmostSameAs)) = findSameAs(middleProjectDS.entityId).unsafeRunSync()
        middleSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
        middleTopmostSameAs.value shouldBe topProjectDS.entityId.value

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe middleProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
      }

    "remove project with all the entities " +
      "and update the sameAs hierarchy " +
      "when both the project and its fork get deleted" in new TestCase {

        val (topProjectDS, topProject) =
          renkuProjectEntities(anyVisibility).addDataset(datasetEntities(provenanceInternal)).generateOne

        val (middleProjectDS, middleProjectBeforeForking) =
          renkuProjectEntities(anyVisibility).importDataset(topProjectDS).generateOne

        val (middleProject, middleProjectFork) = middleProjectBeforeForking.forkOnce()

        val (bottomProjectDS, bottomProject) =
          renkuProjectEntities(anyVisibility).importDataset(middleProjectDS).generateOne

        upload(to = projectsDataset, topProject, middleProject, middleProjectFork, bottomProject)

        givenProjectIdFindingSucceeds(middleProject)
        givenProjectIdFindingSucceeds(middleProjectFork)
        givenSearchGraphsCleaningSucceeds(middleProject)
        givenSearchGraphsCleaningSucceeds(middleProjectFork)

        cleaner.removeTriples(middleProject.path).unsafeRunSync()
        cleaner.removeTriples(middleProjectFork.path).unsafeRunSync()

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        findProject(middleProject.resourceId).unsafeRunSync()     shouldBe List.empty
        findProject(middleProjectFork.resourceId).unsafeRunSync() shouldBe List.empty
        findSameAs(middleProjectDS.entityId).unsafeRunSync()      shouldBe None

        val Some((bottomSameAs, bottomTopmostSameAs)) = findSameAs(bottomProjectDS.entityId).unsafeRunSync()
        bottomSameAs.map(_.value) shouldBe topProjectDS.entityId.value.some
        bottomTopmostSameAs.value shouldBe topProjectDS.entityId.value
      }

    "remove project with all the entities " +
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

        upload(to = projectsDataset, topProject, middleProject, middleProjectFork, bottomProject1, bottomProject2)

        givenProjectIdFindingSucceeds(middleProject)
        givenSearchGraphsCleaningSucceeds(middleProject)

        cleaner.removeTriples(middleProject.path).unsafeRunSync()

        val Some((topSameAs, topTopmostSameAs)) = findSameAs(topProjectDS.entityId).unsafeRunSync()
        topSameAs              shouldBe None
        topTopmostSameAs.value shouldBe topProjectDS.entityId.value

        findProject(middleProject.resourceId).unsafeRunSync()        shouldBe List.empty
        findProject(middleProjectFork.resourceId).unsafeRunSync().size should be > 0

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

  private def findAllData(projectId: projects.ResourceId) = runSelect(
    on = projectsDataset,
    SparqlQuery.of(
      "find all data",
      s"""|SELECT ?s ?p ?o
          |WHERE {
          |  GRAPH <${GraphClass.Project.id(projectId)}> { ?s ?p ?o }
          |}""".stripMargin
    )
  ).map(_.map(_.values.toList))

  private def findProject(projectId: projects.ResourceId) = runSelect(
    on = projectsDataset,
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

  private def findProjectParent(projectId: projects.ResourceId) = runSelect(
    on = projectsDataset,
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

  private def findDataset(datasetId: EntityId) = runSelect(
    on = projectsDataset,
    SparqlQuery.of(
      "find DS triples",
      s"""SELECT ?p ?o
          WHERE {
            GRAPH ?g { <$datasetId> ?p ?o }
          }"""
    )
  ).map(_.map(_.values.toList))

  private def findSameAs(datasetId: EntityId): IO[Option[(Option[datasets.SameAs], TopmostSameAs)]] = runSelect(
    on = projectsDataset,
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

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val projectIdFinder     = mock[ProjectIdFinder[IO]]
    private val searchGraphsCleaner = mock[SearchGraphsCleaner[IO]]
    val cleaner = new TSCleanerImpl[IO](projectIdFinder, searchGraphsCleaner, projectsDSConnectionInfo)

    def givenProjectIdFindingSucceeds(project: Project) =
      (projectIdFinder.findProjectId _)
        .expects(project.path)
        .returning(project.identification.some.pure[IO])

    def givenSearchGraphsCleaningSucceeds(project: Project) =
      (searchGraphsCleaner.cleanSearchGraphs _)
        .expects(project.identification)
        .returning(().pure[IO])
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
