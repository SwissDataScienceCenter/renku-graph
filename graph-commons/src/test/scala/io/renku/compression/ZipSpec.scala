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

package io.renku.compression

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.testtools.IOSpec
import org.scalacheck.Arbitrary
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.io.EOFException
import java.util.zip.ZipException

class ZipSpec extends AnyWordSpec with IOSpec with should.Matchers {

  "zip and unzip" should {
    "zip and unzip the content from String to Byte Array " in {
      val content = nonEmptyStrings().generateOne
      Zip
        .zip[IO](content)
        .flatMap(byteArray => Zip.unzip[IO](byteArray))
        .unsafeRunSync() shouldBe content
    }
  }

  "zip" should {
    "fail with a meaningful error if zipping fails" in {
      val actual = intercept[Exception] {
        Zip.zip[IO](null).unsafeRunSync()
      }

      actual.getMessage       shouldBe "Zipping content failed"
      Option(actual.getCause) shouldBe a[Some[_]]
    }
  }

  "unzip" should {
    "fail with a meaningful error if unzipping fails" in {
      val actual = intercept[Exception] {
        Zip
          .unzip[IO](bytes = Arbitrary.arbByte.arbitrary.generateList(minElements = 10, maxElements = 100).toArray)
          .unsafeRunSync()
      }
      actual.getMessage shouldBe "Unzipping content failed"
      actual.getCause     should (be(a[ZipException]) or be(a[EOFException]))
    }
  }
}
