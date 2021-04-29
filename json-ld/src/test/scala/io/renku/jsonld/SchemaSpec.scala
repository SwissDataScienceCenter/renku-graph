/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

      Schema.from(schemaUrl, separator).toString shouldBe s"$schemaUrl$separator"
    }
  }

  private lazy val schemaUrls: Gen[String] = for {
    baseUrl <- httpUrls()
    path    <- relativePaths(maxSegments = 3)
  } yield s"$baseUrl/$path"
}
