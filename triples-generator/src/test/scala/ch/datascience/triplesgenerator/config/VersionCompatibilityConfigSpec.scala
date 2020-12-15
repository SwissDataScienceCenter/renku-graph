package ch.datascience.triplesgenerator.config

import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.projects.SchemaVersion
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.tuple.Pair
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

class VersionCompatibilityConfigSpec extends AnyWordSpec with should.Matchers {

  "apply" should {

    "fail if there are not value set for the matrix" in {

      val config = ConfigFactory.parseMap(
        Map("compatibility-matrix" -> List.empty[String].asJava).asJava
      )
      val Failure(exception) = VersionCompatibilityConfig[Try](config)
      exception            shouldBe a[Throwable]
      exception.getMessage shouldBe "No compatibility matrix provided for schema version"
    }

  }
}
