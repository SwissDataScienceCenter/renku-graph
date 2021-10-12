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

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class EntityTypeSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "show" should {

    "return the value" in {
      forAll { t: EntityType => t.show shouldBe t.value }
    }
  }

  "EntityTypes.show" should {

    "return string representations of all the types separated with `; `" in {
      forAll { t: EntityTypes => t.show shouldBe t.list.map(_.value).nonEmptyIntercalate("; ") }
    }
  }

  "EntityTypes.equals" should {
    "return true if the types are identical regardless of the order" in {
      forAll { t: EntityTypes =>
        val equalTypes = EntityTypes(NonEmptyList.fromListUnsafe(Random.shuffle(t.list.toList)))
        t            shouldBe equalTypes
        t.hashCode() shouldBe equalTypes.hashCode()
      }
    }

    "return false if the types are not identical" in {
      forAll { (t1: EntityTypes, t2: EntityTypes) =>
        t1            should not be t2
        t1.hashCode() should not be t2.hashCode()
      }
    }
  }

  "contains" should {
    "return true for exact match although in different order" in {
      forAll { entityTypes: EntityTypes =>
        val shuffledTypes = Random.shuffle(entityTypes.toList)
        (entityTypes contains EntityTypes.of(shuffledTypes.head, shuffledTypes.tail: _*)) shouldBe true
      }
    }
    "return true if there are at least the searched types" in {
      forAll { (type1: EntityType, type2: EntityType, type3: EntityType) =>
        EntityTypes.of(type1, type2, type3).contains(type2, type1) shouldBe true
      }
    }
  }
}
