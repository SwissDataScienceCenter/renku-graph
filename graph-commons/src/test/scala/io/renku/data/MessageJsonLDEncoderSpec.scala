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

package io.renku.data

import io.renku.data.MessageJsonLDEncoder._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD}
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MessageJsonLDEncoderSpec extends AnyWordSpec with should.Matchers with EitherValues {

  "Message.Error codecs" should {

    "provide JsonLD encoder producing a JsonLD Entity with renku:Error type and the message as description" in {

      val exception = new Exception(blankStrings().generateOne)
      val message   = Message.Error.fromExceptionMessage(exception)

      val jsonLD = message.asJsonLD

      jsonLD shouldBe JsonLD.entity(
        jsonLD.entityId getOrElse fail("No entityId found"),
        EntityTypes of renku / "Error",
        schema / "description" -> s"${exception.getClass.getName}".asJsonLD
      )
    }
  }

  "Message.Info JsonLD encoder" should {
    "provide JsonLD encoder producing a JsonLD Entity with renku:Info type and the message as description" in {

      val message = Message.Info(nonBlankStrings().generateOne)

      val jsonLD = message.asJsonLD

      jsonLD shouldBe JsonLD.entity(
        jsonLD.entityId getOrElse fail("No entityId found"),
        EntityTypes of renku / "Info",
        schema / "description" -> s"${message.value}".asJsonLD
      )
    }
  }
}
