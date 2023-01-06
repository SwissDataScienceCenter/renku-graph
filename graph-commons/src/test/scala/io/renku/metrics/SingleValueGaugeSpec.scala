/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.metrics

import cats.MonadThrow
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonBlankStrings, sentences}
import org.scalacheck.Arbitrary
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class SingleValueGaugeSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "set" should {

    "set the given value on the gauge" in new TestCase {

      val value = Arbitrary.arbDouble.arbitrary.generateOne

      gauge.set(value) shouldBe MonadThrow[Try].unit

      gauge.wrappedCollector.get() shouldBe value
    }
  }

  private trait TestCase {
    private val name = nonBlankStrings().generateOne
    private val help = sentences().generateOne

    val gauge = new SingleValueGaugeImpl[Try](name, help)
  }
}
