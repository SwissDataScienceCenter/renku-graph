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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.datasets

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.datasets.SameAs
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class KGDatasetInfoFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "findTopmostSameAs" should {

    "return topmostSameAs for the given id" in new TestCase {
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      finder
        .findTopmostSameAs(project.resourceId, dataset.resourceId)
        .unsafeRunSync() shouldBe Set(dataset.provenance.topmostSameAs)
    }

    "return all topmostSameAs for the given id" in new TestCase {
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      val otherTopmostSameAs = datasetTopmostSameAs.generateOne
      insert(to = projectsDataset,
             Quad.edge(GraphClass.Project.id(project.resourceId),
                       dataset.resourceId,
                       renku / "topmostSameAs",
                       otherTopmostSameAs
             )
      )

      upload(to = projectsDataset, project)

      finder
        .findTopmostSameAs(project.resourceId, dataset.resourceId)
        .unsafeRunSync() shouldBe Set(dataset.provenance.topmostSameAs, otherTopmostSameAs)
    }

    "return an empty Set if there is no dataset with the given id" in new TestCase {
      finder
        .findTopmostSameAs(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .unsafeRunSync() shouldBe Set.empty
    }
  }

  "findParentTopmostSameAs" should {

    "return the dataset's topmostSameAs if this dataset has one" in new TestCase {
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      finder.findParentTopmostSameAs(SameAs(dataset.resourceId.asEntityId)).unsafeRunSync() shouldBe
        Some(dataset.provenance.topmostSameAs)
    }

    "return topmostSameAs of the Dataset on the oldest Project using it" in new TestCase {
      val (dataset, parentProject ::~ project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceInternal))
        .forkOnce()
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]],
               _.bimap(_.to[entities.Project], _.to[entities.RenkuProject.WithParent])
        )

      upload(to = projectsDataset, project, parentProject)

      removeTopmostSameAs(GraphClass.Project.id(parentProject.resourceId), dataset.resourceId.asEntityId)
      val oldestProjectTopmostSameAs = datasetTopmostSameAs.generateOne
      insert(projectsDataset,
             Quad.edge(GraphClass.Project.id(parentProject.resourceId),
                       dataset.resourceId,
                       renku / "topmostSameAs",
                       oldestProjectTopmostSameAs
             )
      )

      finder.findParentTopmostSameAs(SameAs(dataset.resourceId.asEntityId)).unsafeRunSync() shouldBe
        Some(oldestProjectTopmostSameAs)
    }

    "return None if there's no dataset with the given id" in new TestCase {
      finder.findParentTopmostSameAs(datasetInternalSameAs.generateOne).unsafeRunSync() shouldBe None
    }

    "return None if there's a dataset with the given id but it has no topmostSameAs" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne

      upload(to = projectsDataset, dataset)

      removeTopmostSameAs(defaultProjectGraphId, dataset.entityId)

      finder.findParentTopmostSameAs(SameAs(dataset.entityId)).unsafeRunSync() shouldBe None
    }
  }

  "findDatasetCreators" should {

    "return all creators' resourceIds" in new TestCase {
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(
          datasetEntities(provenanceNonModified).modify(
            provenanceLens.modify(creatorsLens.modify(_ => personEntities.generateNonEmptyList()))
          )
        )
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      finder.findDatasetCreators(project.resourceId, dataset.resourceId).unsafeRunSync() shouldBe
        dataset.provenance.creators.map(_.resourceId).toList.toSet
    }

    "return no creators if there's no DS with the given id" in new TestCase {
      finder
        .findDatasetCreators(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .unsafeRunSync() shouldBe Set.empty
    }
  }

  "findDatasetOriginalIdentifiers" should {

    "return all DS' Original Identifiers" in new TestCase {
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      val otherOriginalId = datasetOriginalIdentifiers.generateOne
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(project.resourceId),
                  dataset.resourceId,
                  renku / "originalIdentifier",
                  otherOriginalId
             )
      )

      finder.findDatasetOriginalIdentifiers(project.resourceId, dataset.resourceId).unsafeRunSync() shouldBe
        Set(dataset.provenance.originalIdentifier, otherOriginalId)
    }

    "return an empty Set if there's no DS with the given id" in new TestCase {
      finder
        .findDatasetOriginalIdentifiers(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .unsafeRunSync() shouldBe Set.empty
    }
  }

  "findDatasetDateCreated" should {

    "return all DS' dateCreated" in new TestCase {
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance.Internal]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      val otherDateCreated = datasetCreatedDates(min = dataset.provenance.date.instant).generateOne
      insert(
        to = projectsDataset,
        Quad(GraphClass.Project.id(project.resourceId), dataset.resourceId, schema / "dateCreated", otherDateCreated)
      )

      finder.findDatasetDateCreated(project.resourceId, dataset.resourceId).unsafeRunSync() shouldBe
        Set(dataset.provenance.date, otherDateCreated)
    }

    "return an empty Set if there's no DS with the given id" in new TestCase {
      finder
        .findDatasetDateCreated(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .unsafeRunSync() shouldBe Set.empty
    }
  }

  "findDatasetDescriptions" should {

    "return all DS' descriptions" in new TestCase {
      val description1 = datasetDescriptions.generateOne
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSDesc(description1.some)))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      val description2 = datasetDescriptions.generateOne
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(project.resourceId), dataset.resourceId, schema / "description", description2)
      )

      finder.findDatasetDescriptions(project.resourceId, dataset.resourceId).unsafeRunSync() shouldBe
        Set(description1, description2)
    }

    "return an empty Set if requested DS has no description" in new TestCase {
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceNonModified).modify(replaceDSDesc(None)))
        .generateOne
        .bimap(_.to[entities.Dataset[entities.Dataset.Provenance]], _.to[entities.Project])

      upload(to = projectsDataset, project)

      finder.findDatasetDescriptions(project.resourceId, dataset.resourceId).unsafeRunSync() shouldBe Set.empty
    }

    "return an empty Set if there's no DS with the given id" in new TestCase {
      finder
        .findDatasetDescriptions(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .unsafeRunSync() shouldBe Set.empty
    }
  }

  "findDatasetSameAs" should {

    "return all DS' sameAs" in new TestCase {
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

      upload(to = projectsDataset, originalDSProject, importedDSProject)

      val otherSameAs = datasetSameAs.generateOne.entityId
      insert(to = projectsDataset,
             Quad.edge(GraphClass.Project.id(importedDSProject.resourceId),
                       importedDS.resourceId,
                       schema / "sameAs",
                       otherSameAs
             )
      )

      finder.findDatasetSameAs(importedDSProject.resourceId, importedDS.resourceId).unsafeRunSync().map(_.show) shouldBe
        Set(importedDS.provenance.sameAs.entityId, otherSameAs).map(_.show)
    }

    "return no SameAs if there's no DS with the given id" in new TestCase {
      finder
        .findDatasetSameAs(projectResourceIds.generateOne, datasetResourceIds.generateOne)
        .unsafeRunSync() shouldBe Set.empty
    }
  }

  "findWhereNotInvalidated" should {

    "return project resourceIds where DS with the given id is not invalidated" in new TestCase {
      val (ds, project ::~ fork1) =
        anyRenkuProjectEntities
          .addDataset(datasetEntities(provenanceInternal))
          .forkOnce()
          .generateOne

      val forkWithInvalidatedDS = fork1.addDatasets(ds.invalidateNow)

      val (_, (_, forkWithModification)) = project.forkOnce().bimap(identity, _.addDataset(ds.createModification()))

      upload(to = projectsDataset, project, forkWithInvalidatedDS, forkWithModification)

      finder
        .findWhereNotInvalidated(ds.to[entities.Dataset[entities.Dataset.Provenance.Internal]].resourceId)
        .unsafeRunSync() shouldBe Set(project.resourceId, forkWithModification.resourceId)
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val finder = new KGDatasetInfoFinderImpl[IO](projectsDSConnectionInfo)
  }

  private def removeTopmostSameAs(graphId: EntityId, datasetId: EntityId): Unit = runUpdate(
    on = projectsDataset,
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
  ).unsafeRunSync()

  private implicit class SameAsOps(sameAs: SameAs) {
    lazy val entityId: EntityId = sameAs.asJsonLD.entityId.getOrElse(fail("Cannot obtain sameAs @id"))
  }
}
