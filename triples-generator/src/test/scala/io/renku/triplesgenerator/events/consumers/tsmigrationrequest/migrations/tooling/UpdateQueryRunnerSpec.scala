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
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.generators.jsonld.JsonLDGenerators.entityIds
import io.renku.graph.model.Schemas.schema
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class UpdateQueryRunnerSpec extends AsyncWordSpec with AsyncIOSpec with GraphJenaSpec with should.Matchers {

  "run" should {

    "execute the given update query" in projectsDSConfig.use { implicit pcc =>
      val string = nonEmptyStrings().generateOne
      val query = SparqlQuery.of(
        "test query",
        Prefixes of schema -> "schema",
        s"""INSERT DATA { GRAPH <$graphId> { <$entityId> schema:name '$string' } }""".stripMargin
      )

      (runner run query).assertNoException >>
        findString.asserting(_ shouldBe List(string))
    }
  }

  private lazy val graphId  = entityIds.generateOne
  private lazy val entityId = entityIds.generateOne

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def runner(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new UpdateQueryRunnerImpl[IO](pcc)
  }

  private def findString(implicit pcc: ProjectsConnectionConfig) =
    runSelect(
      SparqlQuery.of("find triple",
                     Prefixes of schema -> "schema",
                     s"SELECT ?str WHERE { GRAPH <$graphId> { <$entityId> schema:name ?str } }"
      )
    ).map(_.map(_("str")))
}
