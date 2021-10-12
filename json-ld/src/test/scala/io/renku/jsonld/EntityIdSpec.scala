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

import io.renku.jsonld.EntityId.BlankNodeEntityId
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EntityIdSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "of" should {

    "return an EntityId with the given URI as String" in {
      forAll(httpUrls()) { uri =>
        EntityId.of(uri).value shouldBe uri
      }
    }

    "return an EntityId with the URI created from the given value using the converter" in {
      case class Value(v: String)
      implicit val valueToString: Value => EntityId = v => EntityId.of(v.v)

      forAll(httpUrls() map Value.apply) { value =>
        val entityId = EntityId.of(value)

        entityId       shouldBe a[EntityId]
        entityId.value shouldBe value.v
      }
    }

    "return an EntityId with toString the same as value" in {
      val value = httpUrls().generateOne
      EntityId.of(value).toString shouldBe value
    }
  }

  "blank" should {

    "return an EntityId with a toString as a blank node id" in {
      EntityId.blank.toString should fullyMatch regex "_:.+"
    }

    "return a different value every time it is generated" in {
      EntityId.blank should not be EntityId.blank
    }
  }

  "EntityId json encoder" should {

    import io.circe.Encoder

    "encode the 'of' created EntityId's value as String JSON" in {
      val entityId = EntityId of httpUrls().generateOne
      entityId.asJson shouldBe Encoder.encodeString(entityId.value.toString)
    }

    "encode the 'blank' created EntityId's value as a blank node" in {
      val entityId = EntityId.blank
      entityId.asJson shouldBe Encoder.encodeString(s"_:${entityId.value}")
    }
  }

  "EntityId json decoder" should {

    "decode non blank EntityId as StandardEntityId" in {
      val entityId = EntityId of httpUrls().generateOne
      entityId.asJson.as[EntityId] shouldBe Right(entityId)
    }

    "decode a 'blank' value as a BlankNodeEntityId" in {
      val Right(decodedEntityId) = EntityId.blank.asJson.as[EntityId]
      decodedEntityId shouldBe a[BlankNodeEntityId]
    }
  }
}
