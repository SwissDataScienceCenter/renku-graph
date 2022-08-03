/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
