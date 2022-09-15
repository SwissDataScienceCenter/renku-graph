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

package io.renku.graph.acceptancetests.flows

import io.circe.syntax._
import io.renku.graph.acceptancetests.data.Project
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.http.client.AccessToken
import org.http4s.Status._
import org.scalatest.Assertion
import org.scalatest.matchers.should

trait AccessTokenPresence extends should.Matchers {
  self: GraphServices =>

  def givenAccessTokenPresentFor(project: Project, accessToken: AccessToken): Assertion =
    tokenRepositoryClient
      .PUT(s"projects/${project.id}/tokens", accessToken.asJson, None) shouldBe NoContent
}
