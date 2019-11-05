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
import ch.datascience.generators.CommonGraphGenerators.schemaVersions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.rdfstore.triples.entities.Agent
import ch.datascience.triplesgenerator.reprovisioning.IORdfStoreUpdater
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class OrphanAgentsRemoverSpec extends WordSpec with InMemoryRdfStore {

  "run" should {

    "remove agent triples if the agent is not used anywhere" in new TestCase {

      val schemaVersion = schemaVersions.generateOne
      loadToStore(
        triples(
          List(
            Agent(Agent.Id(schemaVersion))
          )
        )
      )

      triplesFor(schemaVersion).size should be > 0

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      triplesFor(schemaVersion) shouldBe empty
    }

    "do nothing if the agent is used in some other entity" in new TestCase {

      val schemaVersion = schemaVersions.generateOne
      loadToStore(
        triples(
          singleFileAndCommitWithDataset(projectPaths.generateOne, schemaVersion = schemaVersion)
        )
      )

      val agentTriples = triplesFor(schemaVersion)
      agentTriples.size should be > 0

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      triplesFor(schemaVersion) should have size agentTriples.size
    }
  }

  private trait TestCase {
    val triplesRemover = new IORdfStoreUpdater(rdfStoreConfig, TestLogger()) with OrphanAgentsRemover[IO]
  }

  private def triplesFor(schemaVersion: SchemaVersion) =
    runQuery {
      s"""|SELECT ?p 
          |WHERE { ${Agent.Id(schemaVersion).showAs[RdfResource]} ?o ?p }""".stripMargin
    }.unsafeRunSync()
      .map(row => row("p"))
      .toSet
}
