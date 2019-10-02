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

package ch.datascience.triplesgenerator.reprovisioning

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.rdfstore.triples.entities.{DatasetPart, Person, Project}
import io.circe.Json
import io.circe.literal._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class IOOrphanMailtoNoneRemoverSpec extends WordSpec with InMemoryRdfStore {

  "removeOrphanMailtoNoneTriples" should {

    "do nothing if the 'mailto:None' is used an object" in new TestCase {

      loadToStore {
        triples(
          List(
            DatasetPart(
              DatasetPart.Id(commitIds.generateOne, datasetPartLocations.generateOne),
              datasetPartNames.generateOne,
              Project.Id(renkuBaseUrl, projectPaths.generateOne)
            ) deepMerge `schema:creator`(`mailto:None`),
            Person(
              Person.Id(names.generateOne),
              emails.map(Option.apply).generateOne
            ) deepMerge `@id`(`mailto:None`)
          )
        )
      }

      val initialStoreSize = rdfStoreSize

      triplesRemover.removeOrphanMailtoNoneTriples().unsafeRunSync() shouldBe ((): Unit)

      rdfStoreSize shouldBe initialStoreSize
    }

    "remove triples with the 'mailto:None' subject if such a resource is not used an object" in new TestCase {

      val personId = Person.Id(names.generateOne)
      loadToStore {
        triples(
          List(
            DatasetPart(
              DatasetPart.Id(commitIds.generateOne, datasetPartLocations.generateOne),
              datasetPartNames.generateOne,
              Project.Id(renkuBaseUrl, projectPaths.generateOne)
            ) deepMerge `schema:creator`(personId.value),
            Person(
              personId,
              emails.map(Option.apply).generateOne
            ),
            Person(
              Person.Id(names.generateOne),
              emails.map(Option.apply).generateOne
            ) deepMerge `@id`(`mailto:None`)
          )
        )
      }

      mailToNoneTriples should be > 0

      val initialStoreSize = rdfStoreSize

      triplesRemover.removeOrphanMailtoNoneTriples().unsafeRunSync() shouldBe ((): Unit)

      rdfStoreSize      should not be initialStoreSize
      mailToNoneTriples shouldBe 0
    }
  }

  private trait TestCase {
    val triplesRemover = new IOOrphanMailtoNoneRemover(rdfStoreConfig, TestLogger())
  }

  private def mailToNoneTriples =
    runQuery("SELECT (COUNT(*) as ?triples) WHERE { <mailto:None> ?p ?o }")
      .unsafeRunSync()
      .map(row => row("triples"))
      .headOption
      .map(_.toInt)
      .getOrElse(throw new Exception("Cannot find the count of the 'mailto:None' triples"))

  private def `schema:creator`(id: String): Json = json"""{
    "schema:creator": {
        "@id": $id
    }
  }"""

  private def `@id`(id: String): Json = json"""{
    "@id": $id
  }"""

  private val `mailto:None`: String = "mailto:None"
}
