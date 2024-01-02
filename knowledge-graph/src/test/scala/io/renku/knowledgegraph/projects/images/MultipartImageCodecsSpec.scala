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

package io.renku.knowledgegraph.projects.images

import ImageGenerators.images
import MultipartImageCodecs._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.Stream
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{countingGen, nonEmptyStrings}
import org.http4s.MediaType.{application, image}
import org.http4s.headers.`Content-Type`._
import org.http4s.headers.{`Content-Disposition`, `Content-Type`}
import org.http4s.multipart.Part
import org.http4s.{Headers, MediaTypeMismatch}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.typelevel.ci._

class MultipartImageCodecsSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with should.Matchers
    with EitherValues
    with ScalaCheckPropertyChecks {

  forAll(nonEmptyStrings(), images, countingGen) { (partName, image, cnt) =>
    it should s"encode/decode a valid image object #$cnt" in {
      val part = imagePartEncoder[IO](partName, image)
      imagePartDecoder[IO].decode(part, strict = true).value.asserting(_.value shouldBe Some(image))
    }
  }

  it should "decode to None if no data for the image" in {

    val part = Part[IO](
      Headers(`Content-Disposition`("form-data", Map(ci"name" -> nonEmptyStrings().generateOne)),
              `Content-Type`(image.jpeg)
      ),
      Stream.empty
    )
    imagePartDecoder[IO].decode(part, strict = true).value.asserting(_.value shouldBe None)
  }

  it should "fail for if invalid media type" in {

    val partName = nonEmptyStrings().generateOne
    val part = Part[IO](
      Headers(`Content-Type`(application.json), `Content-Disposition`("form-data", Map(ci"name" -> partName))),
      imagePartEncoder[IO](partName, images.generateOne).body
    )

    imagePartDecoder[IO].decode(part, strict = true).value.asserting(_.left.value shouldBe a[MediaTypeMismatch])
  }

  it should "fail if no filename in the Content-Disposition's name parameter" in {

    val partName = nonEmptyStrings().generateOne
    val image    = images.generateOne

    val part = Part[IO](
      Headers(`Content-Type`(image.mediaType)),
      imagePartEncoder[IO](partName, image).body
    )

    imagePartDecoder[IO].decode(part, strict = true).value.asserting {
      _.left.value.message shouldBe "No 'filename' parameter in the 'Content-Disposition' header"
    }
  }
}
