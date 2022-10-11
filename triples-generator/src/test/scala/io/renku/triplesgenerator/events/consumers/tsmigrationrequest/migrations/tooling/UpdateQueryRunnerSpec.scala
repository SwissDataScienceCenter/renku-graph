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
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.generators.jsonld.JsonLDGenerators.entityIds
import io.renku.graph.model.Schemas.schema
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class UpdateQueryRunnerSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "run" should {

    "execute the given update query" in new TestCase {
      val string = nonEmptyStrings().generateOne
      val query = SparqlQuery.of(
        "test query",
        Prefixes of schema -> "schema",
        s"""INSERT DATA { GRAPH <$graphId> { <$entityId> schema:name '$string' } }""".stripMargin
      )

      (runner run query).unsafeRunSync() shouldBe ()

      findString shouldBe List(string)
    }
  }

  private trait TestCase {
    val graphId  = entityIds.generateOne
    val entityId = entityIds.generateOne

    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val runner = new UpdateQueryRunnerImpl[IO](projectsDSConnectionInfo)

    def findString = runSelect(
      on = projectsDataset,
      SparqlQuery.of("find triple",
                     Prefixes of schema -> "schema",
                     s"SELECT ?str WHERE { GRAPH <$graphId> { <$entityId> schema:name ?str } }"
      )
    ).unsafeRunSync()
      .map(_("str"))
  }
}
