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

package ch.datascience.triplesgenerator.eventprocessing

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import ch.datascience.triplesgenerator.rdfstore
import ch.datascience.triplesgenerator.rdfstore.TestData
import org.apache.jena.query.QuerySolution
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOSchemaVersionUpdaterSpec extends WordSpec with InMemoryRdfStore {

  "updateSchemaVersion" should {

    "insert a project triple with the current Schema Version if there's no such a triple yet" in new TestCase {

      loadToStore(rdfstore.TestData.minimalTriples(projectPath, commitIds.generateOne, maybeSchemaVersion = None))

      schemaVersionUpdater.updateSchemaVersion(projectPath).unsafeRunSync() shouldBe ((): Unit)

      runQuery(findingVersion)
        .map(versionToString)
        .unsafeRunSync() shouldBe schemaVersion.toString
    }

    "update the project triples with the current Schema Version if there's already one" in new TestCase {

      val oldSchemaVersion = schemaVersions.generateOne
      loadToStore(rdfstore.TestData.minimalTriples(projectPath, commitIds.generateOne, Some(oldSchemaVersion)))

      schemaVersionUpdater.updateSchemaVersion(projectPath).unsafeRunSync() shouldBe ((): Unit)

      runQuery(findingVersion)
        .map(versionToString)
        .unsafeRunSync() shouldBe schemaVersion.toString
    }

    "do nothing if current Schema Version matches the value in the project triple" in new TestCase {

      loadToStore(rdfstore.TestData.minimalTriples(projectPath, commitIds.generateOne, Some(schemaVersion)))

      schemaVersionUpdater.updateSchemaVersion(projectPath).unsafeRunSync() shouldBe ((): Unit)

      runQuery(findingVersion)
        .map(versionToString)
        .unsafeRunSync() shouldBe schemaVersion.toString
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val schemaVersion = schemaVersions.generateOne
    val projectPath   = projectPaths.generateOne
    val commitId      = commitIds.generateOne

    val schemaVersionUpdater = IOSchemaVersionUpdater(
      context.pure(schemaVersion),
      context.pure(rdfStoreConfig),
      context.pure(TestData.renkuBaseUrl),
      TestLogger()
    ).unsafeRunSync()

    val findingVersion = s"""
    SELECT ?version
    WHERE { <${TestData.renkuBaseUrl / projectPath}> dcterms:hasVersion ?version . }
    """

    val versionToString: QuerySolution => String = _.get("?version").asLiteral().getString
  }
}
