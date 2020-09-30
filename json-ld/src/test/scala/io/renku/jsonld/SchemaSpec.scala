package io.renku.jsonld

import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SchemaSpec extends AnyWordSpec with should.Matchers {

  "asPrefix" should {

    "create a PREFIX phrase" in {
      val schemaUrl = schemaUrls.generateOne
      val separator = Gen.oneOf("/", "#").generateOne
      val schema    = Schema.from(schemaUrl, separator)

      val prefixName = nonEmptyStrings().generateOne

      schema.asPrefix(prefixName) shouldBe s"PREFIX $prefixName: <$schemaUrl$separator>"
    }
  }

  "toString" should {

    "return the value of the Schema" in {
      val schemaUrl = schemaUrls.generateOne
      val separator = Gen.oneOf("/", "#").generateOne
      val schema    = Schema.from(schemaUrl, separator)

      Schema.from(schemaUrl, separator).toString shouldBe s"$schemaUrl$separator"
    }
  }

  private lazy val schemaUrls: Gen[String] = for {
    baseUrl <- httpUrls()
    path    <- relativePaths(maxSegments = 3)
  } yield s"$baseUrl/$path"
}
