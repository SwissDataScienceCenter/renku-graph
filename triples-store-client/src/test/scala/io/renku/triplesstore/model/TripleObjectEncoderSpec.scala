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

package io.renku.triplesstore.model

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.jsonld.JsonLDGenerators._
import io.renku.jsonld.{EntityId, Property}
import org.scalacheck.Arbitrary
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TripleObjectEncoderSpec extends AnyWordSpec with should.Matchers {

  import TripleObjectEncoder.Implicits._

  "TripleObjectEncoder" should {

    "define implicit encoder turning Boolean value to TripleObject.Boolean" in {
      val value = Arbitrary.arbBool.arbitrary.generateOne
      implicitly[TripleObjectEncoder[Boolean]].apply(value) shouldBe TripleObject.Boolean(value)
    }

    "define implicit encoder turning Int value to TripleObject.Int" in {
      val value = Arbitrary.arbInt.arbitrary.generateOne
      implicitly[TripleObjectEncoder[Int]].apply(value) shouldBe TripleObject.Int(value)
    }

    "define implicit encoder turning Long value to TripleObject.Long" in {
      val value = Arbitrary.arbLong.arbitrary.generateOne
      implicitly[TripleObjectEncoder[Long]].apply(value) shouldBe TripleObject.Long(value)
    }

    "define implicit encoder turning Float value to TripleObject.Float" in {
      val value = Arbitrary.arbFloat.arbitrary.generateOne
      implicitly[TripleObjectEncoder[Float]].apply(value) shouldBe TripleObject.Float(value)
    }

    "define implicit encoder turning Double value to TripleObject.Double" in {
      val value = Arbitrary.arbDouble.arbitrary.generateOne
      implicitly[TripleObjectEncoder[Double]].apply(value) shouldBe TripleObject.Double(value)
    }

    "define implicit encoder turning String value to TripleObject.String" in {
      val value = Arbitrary.arbString.arbitrary.generateOne
      implicitly[TripleObjectEncoder[String]].apply(value) shouldBe TripleObject.String(value)
    }

    "define implicit encoder turning EntityId value to TripleObject.Iri" in {
      val value = entityIds.generateOne
      implicitly[TripleObjectEncoder[EntityId]].apply(value) shouldBe TripleObject.Iri(value)
    }

    "define implicit encoder turning Property value to TripleObject.Iri" in {
      val value = properties.generateOne
      implicitly[TripleObjectEncoder[Property]].apply(value) shouldBe TripleObject.Iri(value)
    }
  }

  "TripleObjectEncoder" should {

    "define implicit Contravariant instance" in {
      case class SomeType(value: String)

      val someTypeEncoder: TripleObjectEncoder[SomeType] = stringEncoder.contramap(v => v.value)

      val value = Arbitrary.arbString.arbitrary.generateAs(SomeType)

      someTypeEncoder(value) shouldBe TripleObject.String(value.value)
    }
  }
}
