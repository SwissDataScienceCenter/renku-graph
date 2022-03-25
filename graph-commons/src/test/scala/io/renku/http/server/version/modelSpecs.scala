package io.renku.http.server.version

import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

class ServiceVersionSpec extends AnyWordSpec with should.Matchers with IOSpec {

  "readFromConfig" should {

    "read the value from the 'version' property in the config and turn it into ServiceVersion" in {
      val serviceVersion = serviceVersions.generateOne
      val versionConfig  = ConfigFactory.parseMap(Map("version" -> serviceVersion.show).asJava)

      ServiceVersion.readFromConfig[Try](versionConfig) shouldBe serviceVersion.pure[Try]
    }

    "fail if there's no 'version' property in the config" in {
      val Failure(exception) = ServiceVersion.readFromConfig[Try](ConfigFactory.empty())
      exception.getMessage shouldBe "Key not found: 'version'."
    }

    "fail if 'version' property in the config has illegal value" in {
      val versionConfig = ConfigFactory.parseMap(Map("version" -> "").asJava)

      val Failure(exception) = ServiceVersion.readFromConfig[Try](versionConfig)

      exception.getMessage should startWith("Cannot convert ''")
    }
  }
}

class ServiceNameSpec extends AnyWordSpec with should.Matchers with IOSpec {

  "readFromConfig" should {

    "read the value from the 'service-name' property in the config and turn it into ServiceVersion" in {
      val serviceName = nonEmptyStrings().generateAs(ServiceName)
      val config      = ConfigFactory.parseMap(Map("service-name" -> serviceName.show).asJava)

      ServiceName.readFromConfig[Try](config) shouldBe serviceName.pure[Try]
    }

    "fail if there's no 'service-name' property in the config" in {
      val Failure(exception) = ServiceName.readFromConfig[Try](ConfigFactory.empty())
      exception.getMessage shouldBe "Key not found: 'service-name'."
    }

    "fail if 'service-name' property in the config has illegal value" in {
      val config = ConfigFactory.parseMap(Map("service-name" -> "").asJava)

      val Failure(exception) = ServiceName.readFromConfig[Try](config)

      exception.getMessage should startWith("Cannot convert ''")
    }
  }
}
