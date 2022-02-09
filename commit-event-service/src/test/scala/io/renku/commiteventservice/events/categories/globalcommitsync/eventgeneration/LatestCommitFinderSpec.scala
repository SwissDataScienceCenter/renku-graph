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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.events.CommitId
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Method.GET
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Method, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class LatestCommitFinderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

  "findLatestCommitId" should {
    "return the latest CommitID if remote responds with OK and valid body - access token case" in new TestCase {
      implicit val someAccessToken = accessTokens.generateSome

      val endpointName: String Refined NonEmpty = "commits"

      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[CommitId]])(
          _: Option[AccessToken]
        ))
        .expects(GET, uri"projects" / projectId.show / "repository" / "commits", endpointName, *, someAccessToken)
        .returning(Some(commitId).pure[IO])

      latestCommitFinder.findLatestCommitId(projectId).unsafeRunSync() shouldBe commitId.some
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne
    val commitId  = commitIds.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabClient       = mock[GitLabClient[IO]]
    val latestCommitFinder = new LatestCommitFinderImpl[IO](gitLabClient)
  }
}
