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

package io.renku.graph.model

import cats.data.NonEmptyList
import io.renku.graph.model.projects.Role
import io.renku.graph.model.projects.Role._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class RoleSpec extends AnyFlatSpec with should.Matchers {
  it should "order roles correctly" in {
    val roles = Role.all.sorted
    roles                            shouldBe NonEmptyList.of(Reader, Maintainer, Owner)
    Role.all.toList.sorted           shouldBe roles.toList
    Role.all.toList.sortBy(identity) shouldBe roles.toList
  }
}
