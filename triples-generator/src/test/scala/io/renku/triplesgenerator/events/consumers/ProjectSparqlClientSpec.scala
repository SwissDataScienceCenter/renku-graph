package io.renku.triplesgenerator.events.consumers

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import eu.timepit.refined.auto._
import io.prometheus.client.Histogram
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.projects.{Slug, Visibility}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.projectauth.ProjectAuthData
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.client.util.JenaContainerSupport
import io.renku.triplesstore.{SparqlQuery, SparqlQueryTimeRecorder}
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

  def assertSampled(histogram: Histogram) =
    histogram.collect().get(0).samples.size should be > 0

  def assertNotSampled(histogram: Histogram) =
    histogram.collect().get(0).samples.size shouldBe 0

  def resetHistogram(histogram: Histogram) = {
    histogram.clear()
    assertNotSampled(histogram)
  }

  it should "measure execution time for named queries" in {
    implicit val renkuUrl: RenkuUrl = RenkuUrl("http://localhost")
    val histogram = makeHistogram.unsafeRunSync()
    implicit val sr: SparqlQueryTimeRecorder[IO] = makeSparqlQueryTimeRecorder(histogram)
    withProjectClient.use { c =>
      for {
        _ <- IO(assertNotSampled(histogram))
        up = SparqlQuery.apply(
               name = "test-update",
               prefixes = Set.empty,
               body = sparql"""
                              |PREFIX p: <http://schema.org/>
                              |INSERT DATA {
                              |    p:fred p:hasSpouse p:wilma .
                              |    p:fred p:hasChild p:pebbles .
                              |    p:wilma p:hasChild p:pebbles .
                              |    p:pebbles p:hasSpouse p:bamm-bamm ;
                              |        p:hasChild p:roxy, p:chip.
                              |}""".stripMargin
             )
        _ <- c.update(up)
        _ = assertSampled(histogram)

        _ <- IO(resetHistogram(histogram))
        q = SparqlQuery(
              name = "test-query",
              prefixes = Set.empty,
              body = sparql"SELECT * WHERE { ?s ?p ?o } LIMIT 1"
            )
        _ <- c.query(q)
        _ = assertSampled(histogram)

        _ <- IO(resetHistogram(histogram))
        data = ProjectAuthData(Slug("a/b"), Set.empty, Visibility.Public).asJsonLD
        _ <- c.upload(data)
        _ = assertSampled(histogram)
      } yield ()
    }
  }

  it should "not measure execution time for un-named queries" in {
    val histogram = makeHistogram.unsafeRunSync()
    implicit val sr: SparqlQueryTimeRecorder[IO] = makeSparqlQueryTimeRecorder(histogram)

    withProjectClient.use { c =>
      for {
        _ <- IO(assertNotSampled(histogram))

        q = sparql"""
                    |PREFIX p: <http://schema.org/>
                    |INSERT DATA {
                    |    p:fred p:hasSpouse p:wilma .
                    |    p:fred p:hasChild p:pebbles .
                    |    p:wilma p:hasChild p:pebbles .
                    |    p:pebbles p:hasSpouse p:bamm-bamm ;
                    |        p:hasChild p:roxy, p:chip.
                    |}""".stripMargin

        _ <- c.update(q)

        _ = assertNotSampled(histogram)
      } yield ()
    }
  }

}
