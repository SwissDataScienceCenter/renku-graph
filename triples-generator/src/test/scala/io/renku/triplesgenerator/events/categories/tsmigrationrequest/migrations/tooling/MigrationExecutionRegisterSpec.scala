package io.renku.triplesgenerator.events.categories.tsmigrationrequest
package migrations
package tooling

import Generators._
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuBaseUrls
import io.renku.graph.model.RenkuBaseUrl
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MigrationExecutionRegisterSpec extends AnyWordSpec with IOSpec with InMemoryRdfStore with should.Matchers {

  "MigrationExecutionRegister" should {

    "return true if asked for migration having execution registered" in new TestCase {
      register.registerExecution(migrationName).unsafeRunSync() shouldBe ()

      register.findExecution(migrationName).unsafeRunSync() shouldBe serviceVersion.some
    }

    "return false if asked for migration that wasn't executed" in new TestCase {
      register.findExecution(migrationName).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    val migrationName  = migrationNames.generateOne
    val serviceVersion = serviceVersions.generateOne

    private implicit val renkuBaseUrl: RenkuBaseUrl                = renkuBaseUrls.generateOne
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val register = new MigrationExecutionRegisterImpl[IO](serviceVersion, migrationsStoreConfig)
  }
}
