package io.renku.knowledgegraph.docs

import Encoders._
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.knowledgegraph.docs.model._
import io.swagger.parser.OpenAPIParser
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers._

object OpenApiTester {

  def validateDocument(document: Json): Assertion = Option {
    new OpenAPIParser().readContents(document.noSpaces, null, null).getOpenAPI
  }.nonEmpty shouldBe true

  def validatePath(path: Path): Assertion = validate {
    OpenApiDocument(
      openApiVersion = "3.0.3",
      Info("Knowledge Graph API",
           "Get info about datasets, users, activities, and other entities".some,
           serviceVersions.generateOne.value
      )
    ).addServer(
      Server(
        url = relativePaths().generateOne,
        description = sentences().generateOne.value
      )
    ).addPath(path)
  }

  private def validate(document: OpenApiDocument) = validateDocument(document.asJson)
}
