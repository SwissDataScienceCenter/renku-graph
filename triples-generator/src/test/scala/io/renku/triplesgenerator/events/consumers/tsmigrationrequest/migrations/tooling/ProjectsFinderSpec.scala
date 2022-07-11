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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectsFinderSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with RenkuDataset {

  "findProjects" should {

    "run the configured query and return the fetch the records" in new TestCase {

      val projects = anyProjectEntities.generateNonEmptyList().toList

      projects.foreach(p => upload(to = renkuDataset, p.asJsonLD))

      projectsFinder.findProjects().unsafeRunSync() should contain theSameElementsAs projects.map(_.path)
    }
  }

  private trait TestCase {
    val query = SparqlQuery.of(
      "find path",
      Prefixes of (schema -> "schema", renku -> "renku"),
      """|SELECT ?path
         |WHERE { ?id a schema:Project; renku:projectPath ?path }""".stripMargin
    )
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val projectsFinder = new ProjectsFinderImpl[IO](query, renkuDSConnectionInfo)
  }
}
