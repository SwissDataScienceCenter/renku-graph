/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.reprovisioning.postreprovisioning

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{Identifier, PartLocation}
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.rdfstore.triples.entities._
import ch.datascience.rdfstore.triples.singleFileAndCommitWithDataset.datasetParts
import ch.datascience.triplesgenerator.reprovisioning.IORdfStoreUpdater
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class OrphanPartsRemoverSpec extends WordSpec with InMemoryRdfStore {

  "run" should {

    "remove a Part and all the links to it if the Part is not linked to any project" in new TestCase {

      val datasetId                = datasetIds.generateOne
      val commitId                 = commitIds.generateOne
      val filePath                 = filePaths.generateOne
      val (partName, partLocation) = datasetParts.generateOne

      loadToStore(
        triples(
          Dataset(
            Dataset.Id(datasetId),
            Project.Id(renkuBaseUrl, projectPaths.generateOne),
            datasetNames.generateOne,
            datasetDescriptions.generateOption,
            datasetPublishedDates.generateOption,
            Set.empty,
            List(partName -> partLocation),
            commitId,
            httpUrls.generateOption,
            filePath,
            CommitGeneration.Id(commitId, filePath)
          ) flatMap (_.hcursor.downField("schema:isPartOf").delete.top)
        )
      )

      triplesFor(commitId, partLocation)        should not be empty
      triplesPointingTo(commitId, partLocation) should not be empty

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      triplesFor(commitId, partLocation)        shouldBe empty
      triplesPointingTo(commitId, partLocation) shouldBe empty
    }

    "do not remove a Part if it's linked to some project" in new TestCase {

      val commitId                 = commitIds.generateOne
      val (partName, partLocation) = datasetParts.generateOne

      loadToStore(
        triples(
          List(
            DatasetPart(DatasetPart.Id(commitId, partLocation),
                        partName,
                        Project.Id(renkuBaseUrl, projectPaths.generateOne))
          )
        )
      )

      val initialPartTriples = triplesFor(commitId, partLocation)
      initialPartTriples should not be empty

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      triplesFor(commitId, partLocation) shouldBe initialPartTriples
    }
  }

  private trait TestCase {
    val triplesRemover = new IORdfStoreUpdater(rdfStoreConfig, TestLogger()) with OrphanPartsRemover[IO]
  }

  private def triplesFor(commitId: CommitId, partLocation: PartLocation) =
    runQuery {
      s"""|SELECT DISTINCT ?partResource 
          |WHERE { 
          |  ?partResource rdf:type schema:DigitalDocument .
          |  VALUES ?partResource {${DatasetPart.Id(commitId, partLocation).showAs[RdfResource]}}
          |}""".stripMargin
    }.unsafeRunSync()
      .map(row => row("partResource"))
      .toSet

  private def triplesPointingTo(commitId: CommitId, partLocation: PartLocation) =
    runQuery {
      s"""|SELECT DISTINCT ?p 
          |WHERE { 
          |  ?p ?t ${DatasetPart.Id(commitId, partLocation).showAs[RdfResource]} .
          |}""".stripMargin
    }.unsafeRunSync()
      .map(row => row("p"))
      .toSet
}
