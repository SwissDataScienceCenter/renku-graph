package io.renku.projectauth.sparql

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.graph.model.Schemas
import io.renku.jsonld.{EntityId, EntityTypes, JsonLD, JsonLDEncoder}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.http4s.implicits._
import org.scalatest.flatspec.AsyncFlatSpec
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

class SparqlClientSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]
  val cc = ConnectionConfig(uri"http://localhost:3030/projects", None, None)

  val testQuery = SparqlQuery.raw("""PREFIX schema: <http://schema.org/>
                                    |SELECT * WHERE {
                                    |  graph <https://tygtmzjt:8901/EWxEPoLMmg/projects/123> {
                                    |    ?projId schema:dateModified ?dateModified
                                    |  }
                                    |} LIMIT 100""".stripMargin)

  it should "run sparql queries" in {
    DefaultSparqlClient[IO](cc).use { c =>
      for {
        _ <- c.update(
               SparqlUpdate.raw(
                 """PREFIX schema: <http://schema.org/>
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
             )
        r <- c.query(testQuery)
        obj = r.asObject.getOrElse(sys.error(s"Unexpected response: $r"))
        _   = obj("head").get.isObject    shouldBe true
        _   = obj("results").get.isObject shouldBe true
        _ <- IO.println(r)
        x <- c.queryDecode[Data](testQuery)
        _ <- IO.println(x)
      } yield ()
    }
  }

  it should "upload jsonld" in {
    DefaultSparqlClient[IO](cc).use { c =>
      val data = Data("http://localhost/project/123", Instant.now())
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
        _ <- IO.println(r)
      } yield ()
    }
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
