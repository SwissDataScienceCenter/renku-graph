package io.renku.triplesgenerator.events.categories.cleanup

import cats.effect.IO
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.generators.Generators.fixed
import io.renku.graph.model.GraphModelGenerators.datasetInternalSameAsFrom
import io.renku.graph.model.datasets.{Identifier, TopmostSameAs}
import io.renku.graph.model.projects
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.interpreters.TestLogger
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax.JsonEncoderOps
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks.forAll

class ProjectTriplesRemoverSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryRdfStore
    with EntitiesGenerators {

  "removeTriples" should {

    "remove all activities, datasets and their dependant entities, of a project and the project itself" in new TestCase {
      forAll(projectEntitiesWithDatasetsAndActivities) { projectData =>
        loadToStore(projectData.asJsonLD)

        projectTriplesRemover.removeTriples(projectData.path).unsafeRunSync()

        findAllData.unsafeRunSync() shouldBe List.empty[Map[String, String]]
      }
    }

    "remove all entities which are linked only to the current project " +
      "and the project itself" in new TestCase {
        forAll(projectEntitiesWithDatasetsAndActivities) { project =>
          val (projectData, child) = project.forkOnce()

          loadToStore(projectData.asJsonLD, child.asJsonLD)

          projectTriplesRemover.removeTriples(projectData.path).unsafeRunSync()

          findProject(projectData.path).unsafeRunSync()   shouldBe List.empty
          findProject(child.path).unsafeRunSync().isEmpty shouldBe false
        }
      }
    "update the hierarchy of the datasets and remove all entities which are linked only to the current project " +
      "of a project and the project itself - case when the removed dataset is in the middle of the hierarchy" in new TestCase {
        val topProject = projectEntities(anyVisibility).withDatasets(datasetEntities(provenanceNonModified)).generateOne
        val topDataset = topProject.datasets.head

        val projectToDelete =
          projectWithImportingDataset(topDataset.identification.identifier)().generateOne

        val project = projectWithImportingDataset(projectToDelete.datasets.head.identification.identifier)(
          topDataset.identification.identifier
        ).generateOne
        val dataset = project.datasets.head

        loadToStore(projectToDelete.asJsonLD, project.asJsonLD, topProject.asJsonLD)
        projectTriplesRemover.removeTriples(projectToDelete.path).unsafeRunSync()

        findProject(projectToDelete.path).unsafeRunSync() shouldBe List.empty

        val List(sameAsAndTopmostSameAs) = findDatasetSameAs(dataset.entityId).unsafeRunSync()

        sameAsAndTopmostSameAs.get("sameAs")        shouldBe Some(topDataset.entityId.value)
        sameAsAndTopmostSameAs.get("topmostSameAs") shouldBe Some(topDataset.entityId.value)

      }

    "update the hierarchy of the datasets and remove all entities which are linked only to the current project " +
      "of a project and the project itself - case when the removed dataset is topmost dataset in the hierarchy" in new TestCase {

        val projectToDelete =
          projectEntities(anyVisibility).withDatasets(datasetEntities(provenanceInternal)).generateOne
        val topDataset = projectToDelete.datasets.head

        val project = projectWithImportingDataset(topDataset.identification.identifier)().generateOne
        val dataset = project.datasets.head

        val childProject = projectWithImportingDataset(dataset.identification.identifier)(
          topDataset.identification.identifier
        ).generateOne

        loadToStore(projectToDelete.asJsonLD, project.asJsonLD, childProject.asJsonLD)
        val List(initialChildSameAs) = findDatasetSameAs(childProject.datasets.head.entityId).unsafeRunSync()

        initialChildSameAs.get("sameAs")        shouldBe Some(dataset.entityId.value)
        initialChildSameAs.get("topmostSameAs") shouldBe Some(topDataset.entityId.value)

        projectTriplesRemover.removeTriples(projectToDelete.path).unsafeRunSync()

        findProject(projectToDelete.path).unsafeRunSync() shouldBe List.empty

        val List(sameAsAndTopmostSameAs) = findDatasetSameAs(dataset.entityId).unsafeRunSync()

        sameAsAndTopmostSameAs.get("sameAs")        shouldBe None
        sameAsAndTopmostSameAs.get("topmostSameAs") shouldBe Some(dataset.entityId.value)

        val List(childSameAs) = findDatasetSameAs(childProject.datasets.head.entityId).unsafeRunSync()

        childSameAs.get("sameAs")        shouldBe Some(dataset.entityId.value)
        childSameAs.get("topmostSameAs") shouldBe Some(dataset.entityId.value)
      }
    "update the hierarchy of the datasets and remove all entities which are linked only to the current project " +
      "of a project and the project itself - case when the removed dataset is modified and imported in a different project" in new TestCase {
        val topProject =
          projectEntities(anyVisibility).withDatasets(datasetEntities(provenanceInternal)).generateOne
        val topDataset = topProject.datasets.head

        val (originalDataset, modifiedDataset) = datasetAndModificationEntities(
          provenanceImportedInternalAncestorInternal(
            datasetInternalSameAsFrom(fixed(renkuBaseUrl), fixed(topDataset.identification.identifier)).generateOne
          )
        ).generateOne
        val projectToDelete =
          projectEntities(anyVisibility)
            .withDatasets(_ => fixed(originalDataset), _ => fixed(modifiedDataset))
            .generateOne

        val aProject = projectWithImportingDataset(modifiedDataset.identification.identifier)(
          topDataset.identification.identifier
        ).generateOne
        val dataset = aProject.datasets.head

        loadToStore(projectToDelete.asJsonLD, aProject.asJsonLD)
        val List(initialSameAs) = findDatasetSameAs(dataset.entityId).unsafeRunSync()

        initialSameAs.get("sameAs")        shouldBe Some(modifiedDataset.entityId.value)
        initialSameAs.get("topmostSameAs") shouldBe Some(topDataset.entityId.value)

        projectTriplesRemover.removeTriples(projectToDelete.path).unsafeRunSync()

        findProject(projectToDelete.path).unsafeRunSync() shouldBe List.empty

        val List(sameAsAndTopmostSameAs) = findDatasetSameAs(dataset.entityId).unsafeRunSync()

        sameAsAndTopmostSameAs.get("sameAs")        shouldBe None
        sameAsAndTopmostSameAs.get("topmostSameAs") shouldBe Some(dataset.entityId.value)
      }
  }

  private def projectWithImportingDataset(sameAs: Identifier)(topmostSameAs: Identifier = sameAs) =
    projectEntities(anyVisibility)
      .withDatasets(
        datasetEntities(
          provenanceImportedInternalAncestorInternal(
            datasetInternalSameAsFrom(fixed(renkuBaseUrl), fixed(sameAs)).generateOne,
            datasetInternalSameAsFrom(fixed(renkuBaseUrl), fixed(topmostSameAs))
              .map(sameAs => TopmostSameAs(sameAs.value))
              .generateOne
          )
        )
      )

  private def findAllData = runQuery(s"""SELECT ?s ?p ?o WHERE { ?s ?p ?o .}""")
  private def findProject(projectPath: projects.Path) = runQuery(
    s"""SELECT ?p ?o WHERE { 
       <$renkuBaseUrl/projects/$projectPath> ?p ?o .
       } """
  )
  private def findDatasetSameAs(datasetId: EntityId) = runQuery(
    s"""SELECT ?sameAs ?topmostSameAs WHERE {
        <$datasetId> renku:topmostSameAs ?topmostSameAs . 
        OPTIONAL {<$datasetId> schema:sameAs/schema:url ?sameAs .}                                  
        }
       """
  )
  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO]()
    val sparqlTimeRecorder    = new SparqlQueryTimeRecorder(executionTimeRecorder)
    val projectTriplesRemover = new ProjectTriplesRemoverImpl[IO](
      rdfStoreConfig,
      sparqlTimeRecorder,
      renkuBaseUrl
    )
  }
}
