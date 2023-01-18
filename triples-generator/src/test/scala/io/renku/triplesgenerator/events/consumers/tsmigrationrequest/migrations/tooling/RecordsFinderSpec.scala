/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import io.circe.Decoder
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonBlankStrings, nonEmptyStrings}
import io.renku.generators.jsonld.JsonLDGenerators.entityIds
import io.renku.graph.model.Schemas.schema
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class RecordsFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with IOSpec {

  "findRecords" should {

    "use the given query and decoder and run it against the TS" in new TestCase {

      val graphId  = entityIds.generateOne
      val entityId = entityIds.generateOne
      val name     = nonEmptyStrings().generateOne
      insert(to = projectsDataset, Quad(graphId, entityId, schema / "name", name.asTripleObject))

      implicit val decoder: Decoder[List[String]] =
        ResultsDecoder[List, String](implicit cur => extract[String]("name"))

      client
        .findRecords[String](
          SparqlQuery.of(nonBlankStrings().generateOne,
                         Prefixes of schema -> "schema",
                         "SELECT ?name WHERE { GRAPH ?g { ?s schema:name ?name } }"
          )
        )
        .unsafeRunSync() shouldBe List(name)
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val client = new RecordsFinderImpl[IO](projectsDSConnectionInfo)
  }
}
