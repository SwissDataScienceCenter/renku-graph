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

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.datasets.SameAs
import io.renku.graph.model.testentities.ModelOps
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{GraphClass, entities}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class KGDatasetInfoFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with should.Matchers
    with EntitiesGenerators
    with ModelOps {

  "findTopmostSameAs" should {

    "return topmostSameAs for the given id" in projectsDSConfig.use { implicit pcc =>
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      uploadToProjects(project) >>
        finder
          .findTopmostSameAs(project.resourceId, dataset.resourceId)
          .asserting(_ shouldBe Set(dataset.provenance.topmostSameAs))
    }

    "return all topmostSameAs for the given id" in projectsDSConfig.use { implicit pcc =>
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      val otherTopmostSameAs = datasetTopmostSameAs.generateOne
      insert(
        Quad(GraphClass.Project.id(project.resourceId),
             dataset.resourceId.asEntityId,
             renku / "topmostSameAs",
             otherTopmostSameAs.asEntityId
        )
      ) >>
        uploadToProjects(project) >>
        finder
          .findTopmostSameAs(project.resourceId, dataset.resourceId)
          .asserting(_ shouldBe Set(dataset.provenance.topmostSameAs, otherTopmostSameAs))
    }

    "return an empty Set if there is no dataset with the given id" in projectsDSConfig.use { implicit pcc =>
      finder
        .findTopmostSameAs(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .asserting(_ shouldBe Set.empty)
    }
  }

  "findParentTopmostSameAs" should {

    "return the dataset's topmostSameAs if this dataset has one" in projectsDSConfig.use { implicit pcc =>
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      uploadToProjects(project) >>
        finder
          .findParentTopmostSameAs(SameAs(dataset.resourceId.asEntityId))
          .asserting(_ shouldBe Some(dataset.provenance.topmostSameAs))
    }

    "return topmostSameAs of the Dataset on the oldest Project using it" in projectsDSConfig.use { implicit pcc =>
      val (dataset, parentProject -> project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceInternal))
        .forkOnce()
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]],
               _.bimap(_.to[entities.Project], _.to[entities.RenkuProject.WithParent])
        )

      for {
        _ <- uploadToProjects(project, parentProject)

        _ <- removeTopmostSameAs(GraphClass.Project.id(parentProject.resourceId), dataset.resourceId.asEntityId)
        oldestProjectTopmostSameAs = datasetTopmostSameAs.generateOne
        _ <- insert(
               Quad(GraphClass.Project.id(parentProject.resourceId),
                    dataset.resourceId.asEntityId,
                    renku / "topmostSameAs",
                    oldestProjectTopmostSameAs.asEntityId
               )
             )

        _ <- finder
               .findParentTopmostSameAs(SameAs(dataset.resourceId.asEntityId))
               .asserting(_ shouldBe Some(oldestProjectTopmostSameAs))
      } yield Succeeded
    }

    "return None if there's no dataset with the given id" in projectsDSConfig.use { implicit pcc =>
      finder.findParentTopmostSameAs(datasetInternalSameAs.generateOne).asserting(_ shouldBe None)
    }

    "return None if there's a dataset with the given id but it has no topmostSameAs" in projectsDSConfig.use {
      implicit pcc =>
        val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne

        for {
          _ <- uploadToProjects(dataset)

          _ <- removeTopmostSameAs(defaultProjectGraphId, dataset.entityId)

          _ <- finder.findParentTopmostSameAs(SameAs(dataset.entityId)).asserting(_ shouldBe None)
        } yield Succeeded
    }
  }

  "findDatasetCreators" should {

    "return all creators' resourceIds" in projectsDSConfig.use { implicit pcc =>
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            provenanceLens.modify(creatorsLens.modify(_ => personEntities.generateNonEmptyList()))
          )
        )
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      uploadToProjects(project) >>
        finder
          .findDatasetCreators(project.resourceId, dataset.resourceId)
          .asserting(_ shouldBe dataset.provenance.creators.map(_.resourceId).toList.toSet)
    }

    "return no creators if there's no DS with the given id" in projectsDSConfig.use { implicit pcc =>
      finder
        .findDatasetCreators(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .asserting(_ shouldBe Set.empty)
    }
  }

  "findDatasetOriginalIdentifiers" should {

    "return all DS' Original Identifiers" in projectsDSConfig.use { implicit pcc =>
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      for {
        _ <- uploadToProjects(project)

        otherOriginalId = datasetOriginalIdentifiers.generateOne
        _ <- insert(
               Quad(GraphClass.Project.id(project.resourceId),
                    dataset.resourceId.asEntityId,
                    renku / "originalIdentifier",
                    otherOriginalId.asObject
               )
             )

        _ <- finder
               .findDatasetOriginalIdentifiers(project.resourceId, dataset.resourceId)
               .asserting(_ shouldBe Set(dataset.provenance.originalIdentifier, otherOriginalId))
      } yield Succeeded
    }

    "return an empty Set if there's no DS with the given id" in projectsDSConfig.use { implicit pcc =>
      finder
        .findDatasetOriginalIdentifiers(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .asserting(_ shouldBe Set.empty)
    }
  }

  "findDatasetDateCreated" should {

    "return all DS' dateCreated" in projectsDSConfig.use { implicit pcc =>
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]], _.to[entities.Project])

      for {
        _ <- uploadToProjects(project)

        otherDateCreated = datasetCreatedDates(min = dataset.provenance.date.instant).generateOne
        _ <- insert(
               Quad(GraphClass.Project.id(project.resourceId),
                    dataset.resourceId.asEntityId,
                    schema / "dateCreated",
                    otherDateCreated.asObject
               )
             )

        _ <- finder
               .findDatasetDateCreated(project.resourceId, dataset.resourceId)
               .asserting(_ shouldBe Set(dataset.provenance.date, otherDateCreated))
      } yield Succeeded
    }

    "return an empty Set if there's no DS with the given id" in projectsDSConfig.use { implicit pcc =>
      finder
        .findDatasetDateCreated(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .asserting(_ shouldBe Set.empty)
    }
  }

  "findDatasetDescriptions" should {

    "return all DS' descriptions" in projectsDSConfig.use { implicit pcc =>
      val description1 = datasetDescriptions.generateOne
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSDesc(description1.some)))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      for {
        _ <- uploadToProjects(project)

        description2 = datasetDescriptions.generateOne
        _ <- insert(
               Quad(GraphClass.Project.id(project.resourceId),
                    dataset.resourceId.asEntityId,
                    schema / "description",
                    description2.asObject
               )
             )

        _ <- finder
               .findDatasetDescriptions(project.resourceId, dataset.resourceId)
               .asserting(_ shouldBe Set(description1, description2))
      } yield Succeeded
    }

    "return an empty Set if requested DS has no description" in projectsDSConfig.use { implicit pcc =>
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSDesc(None)))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      uploadToProjects(project) >>
        finder.findDatasetDescriptions(project.resourceId, dataset.resourceId).asserting(_ shouldBe Set.empty)
    }

    "return an empty Set if there's no DS with the given id" in projectsDSConfig.use { implicit pcc =>
      finder
        .findDatasetDescriptions(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .asserting(_ shouldBe Set.empty)
    }
  }

  "findDatasetSameAs" should {

    "return all DS' sameAs" in projectsDSConfig.use { implicit pcc =>
      val (originalDS, originalDSProject) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]], _.to[entities.Project])

      val (importedDS, importedDSProject) = anyRenkuProjectEntities
        .addDataset(
          datasetEntities(provenanceImportedInternalAncestorInternal(fixed(SameAs(originalDS.resourceId.asEntityId))))
        )
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

        _ <- finder
               .findDatasetSameAs(importedDSProject.resourceId, importedDS.resourceId)
               .asserting(_.map(_.show) shouldBe Set(importedDS.provenance.sameAs.entityId, otherSameAs).map(_.show))
      } yield Succeeded
    }

    "return no SameAs if there's no DS with the given id" in projectsDSConfig.use { implicit pcc =>
      finder
        .findDatasetSameAs(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .asserting(_ shouldBe Set.empty)
    }
  }

  "findWhereNotInvalidated" should {

    "return project resourceIds where DS with the given id is not invalidated" in projectsDSConfig.use { implicit pcc =>
      val (ds, project -> fork1) =
        anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceInternal))
          .forkOnce()
          .generateOne

      val forkWithInvalidatedDS = fork1.addDatasets(ds.invalidateNow(personEntities))

      val (_, (_, forkWithModification)) = project.forkOnce().bimap(identity, _.addDataset(ds.createModification()))

      uploadToProjects(project, forkWithInvalidatedDS, forkWithModification) >>
        finder
          .findWhereNotInvalidated(ds.to[entities.Dataset[entities.Dataset.Provenance.Internal]].resourceId)
          .asserting(_ shouldBe Set(project.resourceId, forkWithModification.resourceId))
    }
  }

  "checkPublicationEventsExist" should {

    "return true if there's at least one PublicationEvent for the DS" in projectsDSConfig.use { implicit pcc =>
      val (ds, project) =
        anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceNonModified))
          .suchThat { case (ds, _) => ds.publicationEvents.nonEmpty }
          .generateOne
          .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      uploadToProjects(project) >>
        finder.checkPublicationEventsExist(project.resourceId, ds.resourceId).asserting(_ shouldBe true)
    }

    "return false if there's no PublicationEvents for the DS" in projectsDSConfig.use { implicit pcc =>
      val (ds, project) =
        anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceNonModified).modify(_.copy(publicationEventFactories = Nil)))
          .suchThat { case (ds, _) => ds.publicationEvents.isEmpty }
          .generateOne
          .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      uploadToProjects(project) >>
        finder.checkPublicationEventsExist(project.resourceId, ds.resourceId).asserting(_ shouldBe false)
    }

    "return false if there's no DS with the given id" in projectsDSConfig.use { implicit pcc =>
      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]

      uploadToProjects(project) >>
        finder
          .checkPublicationEventsExist(project.resourceId, datasetResourceIds.generateOne)
          .asserting(_ shouldBe false)
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def finder(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new KGDatasetInfoFinderImpl[IO](pcc)
  }

  private def removeTopmostSameAs(graphId: EntityId, datasetId: EntityId)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Unit] =
    runUpdate(
      SparqlQuery.of(
        name = "topmostSameAs removal",
        Prefixes.of(renku -> "renku", schema -> "schema"),
        body = s"""|DELETE { GRAPH <${graphId.show}> { <$datasetId> renku:topmostSameAs ?sameAs } }
                   |WHERE {
                   |  GRAPH <${graphId.show}> { 
                   |    <$datasetId> a schema:Dataset;
                   |                 renku:topmostSameAs ?sameAs.
                   |  }
                   |}
                   |""".stripMargin
      )
    )

  private implicit class SameAsOps(sameAs: SameAs) {
    lazy val entityId: EntityId = sameAs.asJsonLD.entityId.getOrElse(fail("Cannot obtain sameAs @id"))
  }
}
