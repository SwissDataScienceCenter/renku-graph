package ch.datascience.triplesgenerator.reprovisioning

import cats.data.NonEmptyList
import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.projects.SchemaVersion
import ch.datascience.triplesgenerator.models.RenkuVersionPair
import ch.datascience.generators.Generators.Implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import ch.datascience.triplesgenerator.generators.VersionGenerators._
import eu.timepit.refined.auto._

import scala.util.Try

class ReprovisionJudgeSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "isReprovisioningNeeded" should {
    "return true when the config schema version is different than the current schema version" in new TestCase {
      // Compat matrix
      /**
        *  current cli version is 1.2.1
        *  current schema version 12
        * [1.2.3 -> 13, 1.2.1 -> 12]
        * OR for a RollBack
        * current cli version is 1.2.1
        *  current schema version 12
        * [1.2.3 -> 11, 1.2.1 -> 12]
        */
      val newVersionPair = renkuVersionPairs.generateNonEmptyList(2, 2)
      judge.isReprovisioningNeeded(currentVersionPair, newVersionPair) shouldBe true
    }

    "return true when the last two schema version are the same but the cli versions are different" in new TestCase {
      // Compat matrix
      /**
        *  current cli version is 1.2.1
        *  current schema version 13
        * [1.2.3 -> 13, 1.2.1 -> 13]
        */
      val newVersionPair =
        renkuVersionPairs.generateNonEmptyList(2, 2).map(_.copy(schemaVersion = currentVersionPair.schemaVersion))

      judge.isReprovisioningNeeded(currentVersionPair, newVersionPair) shouldBe true

    }

    "return false when the schema version is the same and the cli version is different" in new TestCase {
      // Compat matrix
      /**
        * current cli version is 1.2.1
        *  current schema version 13
        * [1.2.3 -> 13]
        */
      val newVersionPair = renkuVersionPairs.generateOne.copy(schemaVersion = currentVersionPair.schemaVersion)

      judge.isReprovisioningNeeded(currentVersionPair, NonEmptyList(newVersionPair, Nil)) shouldBe false
    }
  }

  private trait TestCase {
    val currentVersionPair = renkuVersionPairs.generateOne
    val judge              = new ReprovisionJudgeImpl()
  }
}
