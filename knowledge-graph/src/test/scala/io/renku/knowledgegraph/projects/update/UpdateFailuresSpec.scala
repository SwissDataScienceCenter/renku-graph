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

package io.renku.knowledgegraph.projects.update

import Generators.defaultBranchInfos
import cats.syntax.all._
import io.circe.literal._
import io.renku.core.client.Generators.branches
import io.renku.data.Message
import io.renku.generators.Generators.Implicits._
import io.renku.triplesgenerator.api.Generators.{projectUpdatesGen => tgUpdatesGen}
import io.renku.triplesgenerator.api.{ProjectUpdates => TGProjectUpdates}
import org.http4s.Status.Conflict
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UpdateFailuresSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "corePushedToNonDefaultBranch" should {

    "return a Conflict Failure with a JSON message containing the branch Core pushed to" in {
      forAll(tgUpdatesGen, defaultBranchInfos.toGeneratorOfOptions, branches) {
        (tgUpdates, maybeDefaultBranch, corePushBranch) =>
          val failure = UpdateFailures.corePushedToNonDefaultBranch(tgUpdates, maybeDefaultBranch, corePushBranch)

          failure.status shouldBe Conflict

          val updatedValuesInfo =
            if (tgUpdates == TGProjectUpdates.empty) "No values"
            else show"Only $tgUpdates"
          val defaultBranchInfo = maybeDefaultBranch.map(_.branch).fold("")(b => show" '$b'")
          val details =
            show"""|$updatedValuesInfo got updated in the Knowledge Graph due to branch protection rules on the default branch$defaultBranchInfo.
                   | However, an update commit was pushed to a new branch '$corePushBranch' which has to be merged to the default branch with a PR""".stripMargin
              .filter(_ != '\n')

          failure.message shouldBe Message.Error.fromJsonUnsafe {
            json"""{
              "message": $details,
              "branch":  $corePushBranch
            }"""
          }
      }
    }
  }
}
