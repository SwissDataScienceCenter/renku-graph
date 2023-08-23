package io.renku.triplesgenerator.events.consumers

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.prometheus.client.Histogram
import io.renku.graph.model.Schemas
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.{SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.client.util.JenaContainerSupport
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger

class ProjectSparqlClientSpec extends AsyncFlatSpec with AsyncIOSpec with JenaContainerSupport with should.Matchers {
  implicit val logger: Logger[IO] = TestLogger()

  val makeHistogram = IO(
    new Histogram.Builder().name("test").help("test").labelNames("update").buckets(0.5, 0.8).create()
  )

  def makeSparqlQueryTimeRecorder(h: Histogram): SparqlQueryTimeRecorder[IO] =
    new SparqlQueryTimeRecorder[IO](TestExecutionTimeRecorder[IO](Some(h)))

  def withProjectClient(implicit sqr: SparqlQueryTimeRecorder[IO]) =
    withDataset("projects").map(ProjectSparqlClient.apply(_))

  it should "measure execution time for named queries" in {
    val histogram = makeHistogram.unsafeRunSync()
    implicit val sr: SparqlQueryTimeRecorder[IO] = makeSparqlQueryTimeRecorder(histogram)
    withProjectClient.use { c =>
      for {
        a <- IO(histogram.collect())
        _ = a.get(0).samples.size() shouldBe 0

        q = SparqlQuery.apply(
              name = "test-update",
              prefixes = Prefixes
                .of(Schemas.prov -> "prov", Schemas.renku -> "renku", Schemas.schema -> "schema", Schemas.xsd -> "xsd")
                .map(p => Refined.unsafeApply(p.value)),
              body = sparql"""
                             |PREFIX p: <http://bedrock/>
                             |INSERT DATA {
                             |    p:fred p:hasSpouse p:wilma .
                             |    p:fred p:hasChild p:pebbles .
                             |    p:wilma p:hasChild p:pebbles .
                             |    p:pebbles p:hasSpouse p:bamm-bamm ;
                             |        p:hasChild p:roxy, p:chip.
                             |}""".stripMargin
            )
        _ <- c.update(q)

        x = histogram.collect()
        _ = x.get(0).samples.size() should be > 0
      } yield ()
    }
  }

  it should "not measure execution time for un-named queries" in {
    val histogram = makeHistogram.unsafeRunSync()
    implicit val sr: SparqlQueryTimeRecorder[IO] = makeSparqlQueryTimeRecorder(histogram)

    withProjectClient.use { c =>
      for {
        a <- IO(histogram.collect())
        _ = a.get(0).samples.size() shouldBe 0
        q = sparql"""
                    |PREFIX p: <http://bedrock/>
                    |INSERT DATA {
                    |    p:fred p:hasSpouse p:wilma .
                    |    p:fred p:hasChild p:pebbles .
                    |    p:wilma p:hasChild p:pebbles .
                    |    p:pebbles p:hasSpouse p:bamm-bamm ;
                    |        p:hasChild p:roxy, p:chip.
                    |}""".stripMargin

        _ <- c.update(q)

        x = histogram.collect()
        _ = x.get(0).samples.size() shouldBe 0
      } yield ()
    }
  }

}
