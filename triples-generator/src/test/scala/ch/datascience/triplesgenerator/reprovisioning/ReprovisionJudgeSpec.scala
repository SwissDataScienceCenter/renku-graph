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

package ch.datascience.triplesgenerator.reprovisioning

import cats.data.NonEmptyList
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.triplesgenerator.generators.VersionGenerators._
import eu.timepit.refined.auto._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ReprovisionJudgeSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "isReprovisioningNeeded" should {
    "return true when the config schema version is different than the current schema version" in new TestCase {
      // Compat matrix
      /**  current cli version is 1.2.1
        *  current schema version 12
        * [1.2.3 -> 13, 1.2.1 -> 12]
        * OR for a RollBack
        * current cli version is 1.2.1
        *  current schema version 12
        * [1.2.3 -> 11, 1.2.1 -> 12]
        */
      val newVersionPair = renkuVersionPairs.generateNonEmptyList(2, 2)
      judge.isReprovisioningNeeded(currentVersionPair, newVersionPair) shouldBe true
    }

    "return true when the last two schema version are the same but the cli versions are different" in new TestCase {
      // Compat matrix
      /**  current cli version is 1.2.1
        *  current schema version 13
        * [1.2.3 -> 13, 1.2.1 -> 13]
        */
      val newVersionPair =
        renkuVersionPairs.generateNonEmptyList(2, 2).map(_.copy(schemaVersion = currentVersionPair.schemaVersion))

      judge.isReprovisioningNeeded(currentVersionPair, newVersionPair) shouldBe true

    }

    "return false when the schema version is the same and the cli version is different" in new TestCase {
      // Compat matrix
      /** current cli version is 1.2.1
        *  current schema version 13
        * [1.2.3 -> 13]
        */
      val newVersionPair = renkuVersionPairs.generateOne.copy(schemaVersion = currentVersionPair.schemaVersion)

      judge.isReprovisioningNeeded(currentVersionPair, NonEmptyList(newVersionPair, Nil)) shouldBe false
    }
  }

  private trait TestCase {
    val currentVersionPair = renkuVersionPairs.generateOne
    val judge              = new ReprovisionJudgeImpl()
  }
}
