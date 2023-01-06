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

package io.renku.entities.search

import cats.syntax.all._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CriteriaSpec extends AnyWordSpec with should.Matchers {

  "EntityType" should {

    Criteria.Filters.EntityType.all.map {
      case t @ Criteria.Filters.EntityType.Project  => "project"  -> t
      case t @ Criteria.Filters.EntityType.Dataset  => "dataset"  -> t
      case t @ Criteria.Filters.EntityType.Workflow => "workflow" -> t
      case t @ Criteria.Filters.EntityType.Person   => "person"   -> t
    } foreach { case (name, t) =>
      s"be instantiatable from '$name'" in {
        Criteria.Filters.EntityType.from(name) shouldBe t.asRight
      }
    }
  }
}
