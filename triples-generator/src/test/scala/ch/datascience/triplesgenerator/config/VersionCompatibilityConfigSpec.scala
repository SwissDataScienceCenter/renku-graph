package ch.datascience.triplesgenerator.config

import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.projects.SchemaVersion
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

class VersionCompatibilityConfigSpec extends AnyWordSpec with should.Matchers {

  "apply" should {

    "fail if there are not value set for the matrix" in {

      val config = ConfigFactory.parseMap(
        Map(
          "compatibility-matrix" -> List(List("1.1.1", "12")).asJava
        ).asJava
      )
      val Failure(exception) = VersionCompatibilityConfig[Try](config)
      exception shouldBe a[Throwable]
    }

  }
}
