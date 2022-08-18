/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.users

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.GraphModelGenerators.personGitLabIds
import io.renku.graph.model.persons
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class GitLabIdSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "unapply" should {

    "convert valid eventId as string to EventId" in {
      forAll { id: persons.GitLabId =>
        binders.GitLabId.unapply(id.toString) shouldBe Some(id)
      }
    }

    "return None if string value is not a valid GitLabId" in {
      binders.GitLabId.unapply(nonEmptyStrings().generateOne) shouldBe None
    }

    "return None if string value is blank" in {
      binders.GitLabId.unapply(" ") shouldBe None
    }
  }
}
