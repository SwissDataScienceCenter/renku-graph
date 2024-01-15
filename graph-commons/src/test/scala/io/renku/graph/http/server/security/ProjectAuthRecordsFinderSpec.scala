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

package io.renku.graph.http.server.security

import cats.effect._
import io.renku.graph.http.server.security.Authorizer.SecurityRecord
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.projects.Slug
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.projectauth.{Generators, ProjectAuthServiceSupport}
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.SparqlQueryTimeRecorder
import io.renku.triplesstore.client.util.JenaSpec
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger

class ProjectAuthRecordsFinderSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with JenaSpec
    with ProjectAuthServiceSupport
    with should.Matchers {
  implicit val logger:   Logger[IO]                  = TestLogger[IO]()
  implicit val renkuUrl: RenkuUrl                    = RenkuUrl("http://u.rl")
  implicit val qtr:      SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe

  it should "find visibility and project members" in {
    val data = Generators.projectAuthData.stream
    withProjectAuthServiceData(data.take(3)).use { case (s, d) =>
      val finder = ProjectAuthRecordsFinder(s)

      val r      = d.head
      val expect = SecurityRecord(r.visibility, r.slug, r.members.map(_.gitLabId))

      finder(r.slug, None).asserting(list => list shouldBe List(expect))
    }
  }

  it should "find project without members" in {
    val data = Generators.projectAuthData.withMembers().stream
    withProjectAuthServiceData(data.take(1)).use { case (s, d) =>
      val finder = ProjectAuthRecordsFinder(s)
      val r      = d.head
      val expect = SecurityRecord(r.visibility, r.slug, Set.empty)
      finder(r.slug, None).asserting(list => list shouldBe List(expect))
    }
  }

  it should "return empty when project is not found" in {
    withProjectAuthService.use { s =>
      val finder = ProjectAuthRecordsFinder(s)
      val slug   = Slug("p/c")
      finder(slug, None).asserting(_ shouldBe Nil)
    }
  }
}
