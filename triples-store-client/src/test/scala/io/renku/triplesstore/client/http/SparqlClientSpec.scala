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

package io.renku.triplesstore.client.http

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.client.util.TSClientJenaSpec
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

class SparqlClientSpec extends AsyncFlatSpec with AsyncIOSpec with TSClientJenaSpec with should.Matchers {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val testQuery = sparql"""PREFIX schema: <http://schema.org/>
                          |SELECT * WHERE {
                          |  graph <https://tygtmzjt:8901/EWxEPoLMmg/projects/123> {
                          |    ?projId schema:dateModified ?dateModified
                          |  }
                          |} LIMIT 100""".stripMargin

  it should "run sparql queries" in {
    testDSResource.use { c =>
      for {
        _ <- c.update(
               sparql"""PREFIX schema: <http://schema.org/>
                       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                       |INSERT DATA {
                       |  Graph <https://tygtmzjt:8901/EWxEPoLMmg/projects/123> {
                       |     <https://tygtmzjt:8901/EWxEPoLMmg/projects/123>
                       |     schema:dateModified "1988-09-21T17:44:42.325Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>.
                       |  }
                       |}
                       |""".stripMargin
             )
        r <- c.query(testQuery)
        obj = r.asObject.getOrElse(sys.error(s"Unexpected response: $r"))
        _   = obj("head").get.isObject    shouldBe true
        _   = obj("results").get.isObject shouldBe true
        decoded <- c.queryDecode[Data](testQuery)
        _ = decoded shouldBe List(
              Data(
                "https://tygtmzjt:8901/EWxEPoLMmg/projects/123",
                Instant.parse("1988-09-21T17:44:42.325Z")
              )
            )
      } yield ()
    }
  }

  it should "upload jsonld" in {
    val data = Data("http://localhost/project/123", Instant.now())
    testDSResource.use { c =>
      for {
        _ <- c.upload(data.asJsonLD)
        r <- c.queryDecode[Data](SparqlQuery.raw("""PREFIX schema: <http://schema.org/>
                                                   |SELECT ?projId ?dateModified
                                                   |WHERE {
                                                   |  ?p a schema:Person;
                                                   |     schema:dateModified ?dateModified;
                                                   |     schema:project ?projId.
                                                   |}
                                                   |""".stripMargin))
        _ = r.contains(data) shouldBe true
      } yield ()
    }
  }

  object Schemas {
    val renku:  Schema = Schema.from("https://swissdatasciencecenter.github.io/renku-ontology", separator = "#")
    val schema: Schema = Schema.from("http://schema.org")
  }

  case class Data(projId: String, modified: Instant)
  object Data {
    implicit val rowDecoder: RowDecoder[Data] =
      RowDecoder.forProduct2("projId", "dateModified")(Data.apply)

    implicit val jsonLDEncoder: JsonLDEncoder[Data] =
      JsonLDEncoder.instance { data =>
        JsonLD.entity(
          EntityId.blank,
          EntityTypes.of(Schemas.schema / "Person"),
          Schemas.schema / "dateModified" -> data.modified.asJsonLD,
          Schemas.schema / "project"      -> data.projId.asJsonLD
        )
      }
  }
}
