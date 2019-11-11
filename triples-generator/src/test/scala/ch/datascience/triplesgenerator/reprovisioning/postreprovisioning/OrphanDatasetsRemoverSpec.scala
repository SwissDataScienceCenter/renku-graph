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

class OrphanDatasetsRemoverSpec extends WordSpec with InMemoryRdfStore {

  "run" should {

    "remove a dataset if it's not linked to any project and has no parts" in new TestCase {

      val datasetId = datasetIds.generateOne
      val commitId  = commitIds.generateOne
      val filePath  = filePaths.generateOne

      val otherDatasetId = datasetIds.generateOne

      loadToStore(
        triples(
          Dataset(
            Dataset.Id(datasetId),
            Project.Id(renkuBaseUrl, projectPaths.generateOne),
            datasetNames.generateOne,
            datasetDescriptions.generateOption,
            datasetPublishedDates.generateOption,
            Set.empty,
            List.empty,
            commitId,
            httpUrls.generateOption,
            filePath,
            CommitGeneration.Id(commitId, filePath)
          ) flatMap (_.hcursor.downField("schema:isPartOf").delete.top),
          singleFileAndCommitWithDataset(projectPaths.generateOne,
                                         datasetIdentifier = otherDatasetId,
                                         maybeDatasetParts = nonEmptyList(datasetParts).generateOne.toList)
        )
      )

      triplesFor(datasetId) should not be empty

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      triplesFor(datasetId)      shouldBe empty
      triplesFor(otherDatasetId) should not be empty
    }

    "remove a dataset if it's not linked to any project and has some parts" in new TestCase {

      val datasetId = datasetIds.generateOne
      val commitId  = commitIds.generateOne
      val filePath  = filePaths.generateOne
      val parts     = nonEmptyList(datasetParts).generateOne.toList

      loadToStore(
        triples(
          Dataset(
            Dataset.Id(datasetId),
            Project.Id(renkuBaseUrl, projectPaths.generateOne),
            datasetNames.generateOne,
            datasetDescriptions.generateOption,
            datasetPublishedDates.generateOption,
            Set.empty,
            parts,
            commitId,
            httpUrls.generateOption,
            filePath,
            CommitGeneration.Id(commitId, filePath)
          ) flatMap (_.hcursor.downField("schema:isPartOf").delete.top)
        )
      )

      triplesFor(datasetId) should not be empty

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      triplesFor(datasetId) shouldBe empty
      parts foreach {
        case (name, location) =>
          findDatasetPartNames(commitId, location) shouldBe Set(name.toString)
      }
    }

    "do not remove a dataset if it's linked to some projects" in new TestCase {

      val datasetId = datasetIds.generateOne
      val commitId  = commitIds.generateOne
      val filePath  = filePaths.generateOne

      loadToStore(
        triples(
          Dataset(
            Dataset.Id(datasetId),
            Project.Id(renkuBaseUrl, projectPaths.generateOne),
            datasetNames.generateOne,
            datasetDescriptions.generateOption,
            datasetPublishedDates.generateOption,
            Set.empty,
            List.empty,
            commitId,
            httpUrls.generateOption,
            filePath,
            CommitGeneration.Id(commitId, filePath)
          )
        )
      )

      val initialDatasetTriples = triplesFor(datasetId)
      initialDatasetTriples should not be empty

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      triplesFor(datasetId) shouldBe initialDatasetTriples
    }
  }

  private trait TestCase {
    val triplesRemover = new IORdfStoreUpdater(rdfStoreConfig, TestLogger()) with OrphanDatasetsRemover[IO]
  }

  private def triplesFor(datasetId: Identifier) =
    runQuery {
      s"""|SELECT DISTINCT ?p 
          |WHERE { 
          |  ?p rdf:type schema:Dataset ;
          |     schema:identifier '$datasetId' .
          |}""".stripMargin
    }.unsafeRunSync()
      .map(row => row("p"))
      .toSet

  private def findDatasetPartNames(commitId: CommitId, partLocation: PartLocation) =
    runQuery {
      s"""|SELECT DISTINCT ?name 
          |WHERE { 
          |  ${DatasetPart.Id(commitId, partLocation).showAs[RdfResource]} rdf:type schema:DigitalDocument ;
          |                                                                schema:name ?name .
          |}""".stripMargin
    }.unsafeRunSync()
      .map(row => row("name"))
      .toSet
}
