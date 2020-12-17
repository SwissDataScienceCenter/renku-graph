package ch.datascience.triplesgenerator.config

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

class RenkuPythonDevVersionConfigSpec extends AnyWordSpec with should.Matchers {
  "apply" should {
    "return Some(version) if there is a value set" in {
      val version = nonEmptyStrings().generateOne
      val config = ConfigFactory.parseMap(
        Map("renku-python-dev-version" -> version).asJava
      )
      RenkuPythonDevVersionConfig[Try](config) shouldBe Success(Some(RenkuPythonDevVersion(version)))
    }

    "return None if there is no value set" in {
      val config = ConfigFactory.parseMap(
        Map("renku-python-dev-version" -> null).asJava
      )
      RenkuPythonDevVersionConfig[Try](config) shouldBe Success(None)
    }

    "return None if there is an empty string" in {

      val config = ConfigFactory.parseMap(
        Map("renku-python-dev-version" -> nonEmptyStrings().generateOne.map(_ => ' ')).asJava
      )
      RenkuPythonDevVersionConfig[Try](config) shouldBe Success(None)
    }
  }
}
