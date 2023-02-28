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

package io.renku.triplesstore.client.model

import io.renku.triplesstore.client.model.OrderBy.{Direction, Property}
import io.renku.triplesstore.client.sparql.{Fragment, SparqlEncoder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class OrderBySpec extends AnyWordSpec with should.Matchers {

  "OrderBy" should {

    "encode to valid sparql" in {
      val order = OrderBy(
        Direction.Asc(Property("?date")),
        Direction.Desc(Property("?name"))
      )
      val sparql = SparqlEncoder[OrderBy].apply(order)
      sparql shouldBe Fragment("ORDER BY ASC(?date) DESC(?name)")
    }
  }
}
