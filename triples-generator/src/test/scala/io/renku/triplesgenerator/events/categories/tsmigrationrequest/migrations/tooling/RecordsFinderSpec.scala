package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.tooling

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class RecordsFinderSpec extends AnyWordSpec with IOSpec with should.Matchers with InMemoryRdfStore {

  "findRecords" should {

    "run the configured query and return the fetch the records" in new TestCase {

      val projects = anyProjectEntities.generateNonEmptyList().toList

      projects.foreach(p => loadToStore(p.asJsonLD))

      recordsFinder.findRecords().unsafeRunSync() should contain theSameElementsAs projects.map(_.path)
    }
  }

  private trait TestCase {
    val query = SparqlQuery.of(
      "test query",
      Prefixes of (schema -> "schema", renku -> "renku"),
      """|SELECT ?path
         |WHERE { ?id a schema:Project; renku:projectPath ?path }""".stripMargin
    )
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val recordsFinder = new RecordsFinderImpl[IO](query, rdfStoreConfig)
  }
}
