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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class ProjectsFinderSpec extends AsyncWordSpec with AsyncIOSpec with TriplesGeneratorJenaSpec with should.Matchers {

  "findProjects" should {

    "run the configured query and return the fetch the records" in projectsDSConfig.use { implicit pcc =>
      val projects = anyProjectEntities.generateNonEmptyList().toList

      uploadToProjects(projects: _*) >>
        projectsFinder.findProjects.asserting(_ should contain theSameElementsAs projects.map(_.slug))
    }
  }

  private lazy val query = SparqlQuery.of(
    "find slug",
    Prefixes of (schema -> "schema", renku -> "renku"),
    """|SELECT DISTINCT ?slug
       |WHERE { GRAPH ?g { ?id a schema:Project; renku:projectPath ?slug } }""".stripMargin
  )
  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def projectsFinder(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new ProjectsFinderImpl[IO](query, pcc)
  }
}
