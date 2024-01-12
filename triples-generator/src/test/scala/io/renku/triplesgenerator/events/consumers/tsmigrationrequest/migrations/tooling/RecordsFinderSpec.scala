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
import io.circe.Decoder
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonBlankStrings, nonEmptyStrings}
import io.renku.generators.jsonld.JsonLDGenerators.entityIds
import io.renku.graph.model.Schemas.schema
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore.ResultsDecoder._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class RecordsFinderSpec extends AsyncWordSpec with AsyncIOSpec with TriplesGeneratorJenaSpec with should.Matchers {

  "findRecords" should {

    "use the given query and decoder and run it against the TS" in projectsDSConfig.use { implicit pcc =>
      val graphId  = entityIds.generateOne
      val entityId = entityIds.generateOne
      val name     = nonEmptyStrings().generateOne
      implicit val decoder: Decoder[List[String]] =
        ResultsDecoder[List, String](implicit cur => extract[String]("name"))

      for {
        _ <- insert(Quad(graphId, entityId, schema / "name", name.asTripleObject))

        _ <- client
               .findRecords[String](
                 SparqlQuery.of(nonBlankStrings().generateOne,
                                Prefixes of schema -> "schema",
                                "SELECT ?name WHERE { GRAPH ?g { ?s schema:name ?name } }"
                 )
               )
               .asserting(_ shouldBe List(name))
      } yield Succeeded
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def client(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new RecordsFinderImpl[IO](pcc)
  }
}
