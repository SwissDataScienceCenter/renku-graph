package ch.datascience.webhookservice.config

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.collection.JavaConverters._

class ConfigOpsSpec extends WordSpec {

  import ConfigOps.Implicits._

  "get" should {

    "be able to find String values" in {
      val config = ConfigFactory.parseMap(
        Map("key" -> "value").asJava
      )

      config.get[String]("key") shouldBe "value"
    }

    "throw an ConfigException.Missing when there's no key for String value" in {
      an[ConfigException.Missing] should be thrownBy emptyConfig.get[String]("key")
    }

    "be able to find Int values" in {
      val config = ConfigFactory.parseMap(
        Map("key" -> 2).asJava
      )

      config.get[Int]("key") shouldBe 2
    }

    "throw an ConfigException.Missing when there's no key for Int value" in {
      an[ConfigException.Missing] should be thrownBy emptyConfig.get[Int]("key")
    }
  }

  private val emptyConfig = ConfigFactory.empty()
}
