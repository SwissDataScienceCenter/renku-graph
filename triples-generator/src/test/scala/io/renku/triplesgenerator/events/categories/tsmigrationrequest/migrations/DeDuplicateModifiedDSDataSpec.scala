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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectResourceIds
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling.UpdateQueryMigration

class DeDuplicateModifiedDSDataSpec extends AnyWordSpec with should.Matchers with IOSpec with InMemoryRdfStore {

  "query" should {

    "find modified datasets and remove " +
      "all originalIdentifiers duplicates that are the same as the dataset identifier, " +
      "all topmostDerivedFrom duplicates that are pointing back to the dataset, " +
      "all topmostSameAs duplicates that are NOT pointing back to the dataset" in {
        // project with DS having duplicate originalIdentifier, topmostDerivedFrom and topmostSameAs
        val (project1DS, project1) = {
          val (_ ::~ modifiedDS, proj: RenkuProject.WithoutParent) = renkuProjectEntities(anyVisibility)
            .addDatasetAndModification(datasetEntities(provenanceInternal))
            .generateOne

          val breakingModification = modifiedDS.copy(provenance =
            modifiedDS.provenance.copy(
              initialVersion = datasets.InitialVersion(modifiedDS.identifier),
              topmostDerivedFrom =
                datasets.TopmostDerivedFrom(datasets.DerivedFrom(Dataset.entityId(modifiedDS.identifier)))
            )
          )
          modifiedDS.to[entities.Dataset[entities.Dataset.Provenance.Modified]] -> proj
            .copy(datasets = breakingModification :: proj.datasets)
            .asInstanceOf[RenkuProject]
            .to[entities.RenkuProject]
        }

        // project with DS having duplicate originalIdentifier and topmostDerivedFrom
        val (project2DS, project2) = {
          val (_ ::~ modifiedDS, proj: RenkuProject.WithoutParent) = renkuProjectEntities(anyVisibility)
            .addDatasetAndModification(datasetEntities(provenanceInternal))
            .generateOne

          val breakingModification = modifiedDS.copy(provenance =
            modifiedDS.provenance.copy(
              initialVersion = datasets.InitialVersion(modifiedDS.identifier),
              topmostDerivedFrom =
                datasets.TopmostDerivedFrom(datasets.DerivedFrom(Dataset.entityId(modifiedDS.identifier)))
            )
          )
          modifiedDS.to[entities.Dataset[entities.Dataset.Provenance.Modified]] -> proj
            .copy(datasets = breakingModification :: proj.datasets)
            .asInstanceOf[RenkuProject]
            .to[entities.RenkuProject]
        }

        // project with no duplicates
        val _ ::~ project3DS ::~ project3 = renkuProjectEntities(anyVisibility)
          .addDatasetAndModification(datasetEntities(provenanceInternal))
          .generateOne

        loadToStore(project1.asJsonLD)
        loadToStore(
          JsonLD.edge(project1DS.resourceId.asEntityId,
                      renku / "topmostSameAs",
                      projectResourceIds.generateOne.asEntityId
          )
        )
        loadToStore(project2.asJsonLD)
        loadToStore(project3.asJsonLD)

        findOriginalIdentifiers(project1DS.identification.identifier).size shouldBe 2
        findTopmostDerivedFrom(project1DS.identification.identifier).size  shouldBe 2
        findTopmostSameAs(project1DS.identification.identifier).size       shouldBe 2

        findOriginalIdentifiers(project2DS.identification.identifier).size shouldBe 2
        findTopmostDerivedFrom(project2DS.identification.identifier).size  shouldBe 2
        findTopmostSameAs(project2DS.identification.identifier).size       shouldBe 1

        findOriginalIdentifiers(project3DS.identification.identifier).size shouldBe 1
        findTopmostDerivedFrom(project3DS.identification.identifier).size  shouldBe 1
        findTopmostSameAs(project3DS.identification.identifier).size       shouldBe 1

        runUpdate(DeDuplicateModifiedDSData.query).unsafeRunSync() shouldBe ()

        findOriginalIdentifiers(project1DS.identification.identifier) shouldBe Set(project1DS.provenance.initialVersion)
        findTopmostDerivedFrom(project1DS.identification.identifier) shouldBe Set(
          project1DS.provenance.topmostDerivedFrom
        )
        findTopmostSameAs(project1DS.identification.identifier) shouldBe Set(project1DS.provenance.topmostSameAs)

        findOriginalIdentifiers(project2DS.identification.identifier) shouldBe Set(project2DS.provenance.initialVersion)
        findTopmostDerivedFrom(project2DS.identification.identifier) shouldBe Set(
          project2DS.provenance.topmostDerivedFrom
        )
        findTopmostSameAs(project2DS.identification.identifier).size shouldBe 1

        findOriginalIdentifiers(project3DS.identification.identifier).size shouldBe 1
        findTopmostDerivedFrom(project3DS.identification.identifier).size  shouldBe 1
        findTopmostSameAs(project3DS.identification.identifier).size       shouldBe 1
      }
  }

  "apply" should {
    "return an QueryBasedMigration" in {
      implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
      implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
      DeDuplicateModifiedDSData[IO].unsafeRunSync().getClass shouldBe classOf[UpdateQueryMigration[IO]]
    }
  }

  private def findOriginalIdentifiers(id: datasets.Identifier): Set[datasets.InitialVersion] =
    runQuery(s"""|SELECT ?original 
                 |WHERE { 
                 |  ?id a schema:Dataset;
                 |  schema:identifier '$id';
                 |  renku:originalIdentifier ?original
                 |}""".stripMargin)
      .unsafeRunSync()
      .map(row => datasets.InitialVersion(row("original")))
      .toSet

  private def findTopmostDerivedFrom(id: datasets.Identifier): Set[datasets.TopmostDerivedFrom] =
    runQuery(s"""|SELECT ?derivedFrom 
                 |WHERE { 
                 |  ?id a schema:Dataset;
                 |  schema:identifier '$id';
                 |  renku:topmostDerivedFrom ?derivedFrom
                 |}""".stripMargin)
      .unsafeRunSync()
      .map(row => datasets.TopmostDerivedFrom(row("derivedFrom")))
      .toSet

  private def findTopmostSameAs(id: datasets.Identifier): Set[datasets.TopmostSameAs] =
    runQuery(s"""|SELECT ?sameAs 
                 |WHERE { 
                 |  ?id a schema:Dataset;
                 |  schema:identifier '$id';
                 |  renku:topmostSameAs ?sameAs
                 |}""".stripMargin)
      .unsafeRunSync()
      .map(row => datasets.TopmostSameAs(row("sameAs")))
      .toSet
}
