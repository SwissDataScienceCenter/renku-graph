package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators.renkuBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ReProvisioningFlagSetterSpec extends AnyWordSpec with should.Matchers with InMemoryRdfStore {

  "setUnderReProvisioningFlag" should {

    "insert the ReProvisioningJsonLD object" in new TestCase {

      findFlag shouldBe false

      flagSetter.setUnderReProvisioningFlag().unsafeRunSync() shouldBe ((): Unit)

      findFlag shouldBe true
    }
  }

  private trait TestCase {
    private val renkuBaseUrl = renkuBaseUrls.generateOne
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))

    val flagSetter = new ReProvisioningFlagSetterImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }

  private def findFlag: Boolean =
    runQuery(s"""|SELECT DISTINCT ?flag
                 |WHERE {
                 |  ?id rdf:type renku:ReProvisioning;
                 |      renku:currentlyReProvisioning ?flag
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row("flag").toBoolean)
      .headOption
      .getOrElse(false)
}
