/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesstore.client.http.jsonld

import cats.effect.IO
import fs2.text
import io.renku.generators.Generators.Implicits._
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.testtools.IOSpec
import org.http4s.headers.`Content-Type`
import org.http4s.{Headers, MediaType}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class JsonLDEntityCodecSpec extends AnyWordSpec with should.Matchers with IOSpec with JsonLDEntityCodec {

  "jsonLDEncoder.toEntity" should {

    "produce EntityEncoder with an Entity object with JsonLD payload serialized to String" in {
      val jsonLD = jsonLDEntities.generateOne

      jsonLDEncoder[IO]
        .toEntity(jsonLD)
        .body
        .through(text.utf8.decode)
        .through(text.lines)
        .compile
        .string
        .unsafeRunSync() shouldBe jsonLD.toJson.noSpaces
    }
  }

  "jsonLDEncoder" should {

    "produce EntityEncoder with Content-Type: application/ld+json" in {
      val expected = `Content-Type`(MediaType.application.`ld+json`)
      jsonLDEncoder[IO].headers     shouldBe Headers(expected)
      jsonLDEncoder[IO].contentType shouldBe Some(expected)
    }
  }
}
