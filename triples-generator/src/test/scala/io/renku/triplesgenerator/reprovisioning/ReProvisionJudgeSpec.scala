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

package io.renku.triplesgenerator.reprovisioning

import cats.data.NonEmptyList
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuVersionPair
import io.renku.triplesgenerator.generators.VersionGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ReProvisionJudgeSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "reProvisioningNeeded" should {

    "return true when the config schema version is different than the current schema version" in new TestCase {
      // Compat matrix
      /** current cli version is 1.2.1 current schema version 12 [1.2.3 -> 13, 1.2.1 -> 12] OR for a RollBack current
        * cli version is 1.2.1 current schema version 12 [1.2.3 -> 11, 1.2.1 -> 12]
        */

      val currentVersionPair = renkuVersionPairs.generateOne
      (versionPairFinder.find _).expects().returning(currentVersionPair.some.pure[Try])

      val newVersionPair = renkuVersionPairs.generateNonEmptyList(2, 2)

      judge(newVersionPair).reProvisioningNeeded() shouldBe true.pure[Try]
    }

    "return true when the last two schema versions are the same but the cli versions are different" in new TestCase {
      // Compat matrix
      /** current cli version is 1.2.1 current schema version 13 [1.2.3 -> 13, 1.2.1 -> 13] */

      val currentVersionPair = renkuVersionPairs.generateOne
      (versionPairFinder.find _).expects().returning(currentVersionPair.some.pure[Try])

      val newVersionPair =
        renkuVersionPairs.generateNonEmptyList(2, 2).map(_.copy(schemaVersion = currentVersionPair.schemaVersion))

      judge(newVersionPair).reProvisioningNeeded() shouldBe true.pure[Try]
    }

    "return false when the schema version is the same and the cli version is different" in new TestCase {

      /** current cli version is 1.2.1 current schema version 13 Compat matrix: [1.2.3 -> 13] */

      val currentVersionPair = renkuVersionPairs.generateOne
      (versionPairFinder.find _).expects().returning(currentVersionPair.some.pure[Try])

      val newVersionPair = renkuVersionPairs.generateOne.copy(schemaVersion = currentVersionPair.schemaVersion)

      judge(NonEmptyList.one(newVersionPair)).reProvisioningNeeded() shouldBe false.pure[Try]
    }

    "return true when the current schema version is None" in new TestCase {
      (versionPairFinder.find _).expects().returning(Option.empty[RenkuVersionPair].pure[Try])

      judge(NonEmptyList.one(renkuVersionPairs.generateOne)).reProvisioningNeeded() shouldBe true.pure[Try]
    }
  }

  private trait TestCase {
    val versionPairFinder = mock[RenkuVersionPairFinder[Try]]
    def judge(compatibilityMatrix: NonEmptyList[RenkuVersionPair]) =
      new ReProvisionJudgeImpl(versionPairFinder, compatibilityMatrix)
  }
}
