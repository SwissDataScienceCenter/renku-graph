package ch.datascience.triplesgenerator.reprovisioning

import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.triplesgenerator.models.RenkuVersionPair
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.SchemaVersion
import ch.datascience.triplesgenerator.generators.VersionGenerators._
import eu.timepit.refined.auto._

import scala.util.Try

class RenkuVersionPairUpdaterSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {
  "updater" should {
    "create a renku:VersionPair with the given version pair" in new TestCase {
      ???
    }
  }

  private trait TestCase {
    val currentRenkuVersionPair = renkuVersionPairs.generateOne

    val versionCompatibilityPairs = renkuVersionPairs.generateNonEmptyList(2).toList

    def newCreator(versionPair: RenkuVersionPair): RenkuVersionPairUpdater[Try] =
      new RenkuVersionPairUpdaterImpl[Try](versionCompatibilityPairs)

    private def findPairInDb: Set[RenkuVersionPair] =
      runQuery(s"""|SELECT DISTINCT ?versionPair
                   |WHERE {
                   |   ?id rdf:type renku:VersionPair;
                   |       renku:versionPair ?versionPair.
                   |}
                   |""".stripMargin)
        .unsafeRunSync()
        .map { row =>
          row("versionPair").split(":").toList match {
            case List(cliVersion, schemaVersion) =>
              RenkuVersionPair(CliVersion(cliVersion), SchemaVersion(schemaVersion))
            case _ => throw new Exception("")
          }
        }
        .toSet
  }
}
