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

package io.renku.graph.model.views

import io.renku.tinytypes.{TinyType, TinyTypeFactory}
import io.renku.generators.Generators.Implicits._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import io.renku.triplesstore.client.syntax._

class TinyTypeToObjectSpec extends AnyWordSpec with should.Matchers {

  "asTripleObject" should {

    import io.renku.triplesstore.client.model.{TripleObject, TripleObjectEncoder}

    import java.util.UUID

    "return a TripleObjectEncoder for the TinyType if encoder for its value is available" in {

      case class SomeTinyType(v: UUID) extends TinyType {
        type V = UUID
        override val value: UUID = v
      }
      object SomeTinyType extends TinyTypeFactory[SomeTinyType](new SomeTinyType(_))

      implicit val uuidEncoder: TripleObjectEncoder[UUID] =
        TripleObjectEncoder.instance(v => TripleObject.String(v.toString))

      val value = Gen.uuid.generateOne
      val tt    = SomeTinyType(value)

      tt.asObject shouldBe TripleObject.String(value.toString)
    }
  }
}
