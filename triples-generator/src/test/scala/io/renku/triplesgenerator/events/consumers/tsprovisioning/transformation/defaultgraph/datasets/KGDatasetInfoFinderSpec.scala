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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.defaultgraph.datasets

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.datasets.SameAs
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
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
    with RenkuDataset {

  "findTopmostSameAs" should {

    "return topmostSameAs for the given id" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      upload(to = renkuDataset, dataset)

      finder
        .findTopmostSameAs(dataset.resourceId)
        .unsafeRunSync() shouldBe Set(dataset.provenance.topmostSameAs)
    }

    "return all topmostSameAs for the given id" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      val otherTopmostSameAs = datasetTopmostSameAs.generateOne
      insert(to = renkuDataset, Triple.edge(dataset.resourceId, renku / "topmostSameAs", otherTopmostSameAs))

      upload(to = renkuDataset, dataset)

      finder
        .findTopmostSameAs(dataset.resourceId)
        .unsafeRunSync() shouldBe Set(dataset.provenance.topmostSameAs, otherTopmostSameAs)
    }

    "return an empty Set if there is no dataset with the given id" in new TestCase {
      finder.findTopmostSameAs(datasetResourceIds.generateOne).unsafeRunSync() shouldBe Set.empty
    }
  }

  "findParentTopmostSameAs" should {

    "return the dataset's topmostSameAs if this dataset has one" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne

      upload(to = renkuDataset, dataset)

      finder.findParentTopmostSameAs(SameAs(dataset.entityId)).unsafeRunSync() shouldBe
        Some(dataset.provenance.topmostSameAs)
    }

    "return None if there's no dataset with the given id" in new TestCase {
      finder.findParentTopmostSameAs(datasetInternalSameAs.generateOne).unsafeRunSync() shouldBe None
    }

    "return None if there's a dataset with the given id but it has no topmostSameAs" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne

      upload(to = renkuDataset, dataset)

      removeTopmostSameAs(dataset.entityId)

      finder.findParentTopmostSameAs(SameAs(dataset.entityId)).unsafeRunSync() shouldBe None
    }

    "fail if the dataset has multiple topmostSameAs" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne

      upload(to = renkuDataset, dataset)

      insert(to = renkuDataset,
             Triple.edge(dataset.entityId, renku / "topmostSameAs", datasetTopmostSameAs.generateOne.asEntityId)
      )

      val sameAs = SameAs(dataset.entityId)

      val exception = intercept[Exception] {
        finder.findParentTopmostSameAs(sameAs).unsafeRunSync()
      }

      exception.getMessage should include(s"Multiple topmostSameAs found for dataset ${sameAs.show}")
    }
  }

  "findDatasetCreators" should {

    "return all creators' resourceIds" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified)
        .modify(provenanceLens.modify(creatorsLens.modify(_ => personEntities.generateNonEmptyList())))
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      upload(to = renkuDataset, dataset)

      finder.findDatasetCreators(dataset.resourceId).unsafeRunSync() shouldBe
        dataset.provenance.creators.map(_.resourceId).toList.toSet
    }

    "return no creators if there's no DS with the given id" in new TestCase {
      finder.findDatasetCreators(datasetResourceIds.generateOne).unsafeRunSync() shouldBe Set.empty
    }
  }

  "findDatasetOriginalIdentifiers" should {

    "return all DS' Original Identifiers" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      upload(to = renkuDataset, dataset)

      val otherOriginalId = datasetOriginalIdentifiers.generateOne
      insert(to = renkuDataset, Triple(dataset.resourceId, renku / "originalIdentifier", otherOriginalId))

      finder.findDatasetOriginalIdentifiers(dataset.resourceId).unsafeRunSync() shouldBe
        Set(dataset.provenance.originalIdentifier, otherOriginalId)
    }

    "return an empty Set if there's no DS with the given id" in new TestCase {
      finder
        .findDatasetOriginalIdentifiers(datasetResourceIds.generateOne)
        .unsafeRunSync() shouldBe Set.empty
    }
  }

  "findDatasetDateCreated" should {

    "return all DS' dateCreated" in new TestCase {
      val dataset = datasetEntities(provenanceInternal).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Internal]]

      upload(to = renkuDataset, dataset)

      val otherDateCreated = datasetCreatedDates(min = dataset.provenance.date.instant).generateOne
      insert(to = renkuDataset, Triple(dataset.resourceId, schema / "dateCreated", otherDateCreated))

      finder.findDatasetDateCreated(dataset.resourceId).unsafeRunSync() shouldBe
        Set(dataset.provenance.date, otherDateCreated)
    }

    "return an empty Set if there's no DS with the given id" in new TestCase {
      finder.findDatasetDateCreated(datasetResourceIds.generateOne).unsafeRunSync() shouldBe Set.empty
    }
  }

  "findDatasetDescriptions" should {

    "return all DS' descriptions" in new TestCase {
      val description1 = datasetDescriptions.generateOne
      val dataset = datasetEntities(provenanceNonModified)
        .modify(replaceDSDesc(description1.some))
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      upload(to = renkuDataset, dataset)

      val description2 = datasetDescriptions.generateOne
      insert(to = renkuDataset, Triple(dataset.resourceId, schema / "description", description2))

      finder.findDatasetDescriptions(dataset.resourceId).unsafeRunSync() shouldBe
        Set(description1, description2)
    }

    "return an empty Set if requested DS has no description" in new TestCase {
      val dataset = datasetEntities(provenanceNonModified)
        .modify(replaceDSDesc(None))
        .decoupledFromProject
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      upload(to = renkuDataset, dataset)

      finder.findDatasetDescriptions(dataset.resourceId).unsafeRunSync() shouldBe Set.empty
    }

    "return an empty Set if there's no DS with the given id" in new TestCase {
      finder
        .findDatasetDescriptions(datasetResourceIds.generateOne)
        .unsafeRunSync() shouldBe Set.empty
    }
  }

  "findDatasetSameAs" should {

    "return all DS' sameAs" in new TestCase {
      val originalDS = datasetEntities(provenanceInternal).decoupledFromProject.generateOne
      val importedDS = datasetEntities(
        provenanceImportedInternalAncestorInternal(fixed(SameAs(originalDS.entityId)))
      ).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]

      upload(to = renkuDataset, originalDS)
      upload(to = renkuDataset, importedDS)

      val otherSameAs = datasetSameAs.generateOne.entityId
      insert(to = renkuDataset, Triple.edge(importedDS.resourceId, schema / "sameAs", otherSameAs))

      finder.findDatasetSameAs(importedDS.resourceId).unsafeRunSync().map(_.show) shouldBe
        Set(importedDS.provenance.sameAs.entityId, otherSameAs).map(_.show)
    }

    "return no SameAs if there's no DS with the given id" in new TestCase {
      finder.findDatasetSameAs(datasetResourceIds.generateOne).unsafeRunSync() shouldBe Set.empty
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val finder = new KGDatasetInfoFinderImpl[IO](renkuDSConnectionInfo)
  }

  private def removeTopmostSameAs(datasetId: EntityId): Unit = runUpdate(
    on = renkuDataset,
    SparqlQuery.of(
      name = "topmostSameAs removal",
      Prefixes.of(renku -> "renku", schema -> "schema"),
      body = s"""|DELETE { <$datasetId> renku:topmostSameAs ?sameAs }
                 |WHERE {
                 |  <$datasetId> a schema:Dataset;
                 |               renku:topmostSameAs ?sameAs.
                 |}
                 |""".stripMargin
    )
  ).unsafeRunSync()

  private implicit class SameAsOps(sameAs: SameAs) {
    lazy val entityId: EntityId = sameAs.asJsonLD.entityId.getOrElse(fail("Cannot obtain sameAs @id"))
  }
}
