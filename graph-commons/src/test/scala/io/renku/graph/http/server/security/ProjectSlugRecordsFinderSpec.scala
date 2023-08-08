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

package io.renku.graph.http.server.security

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.http.server.security.Authorizer.SecurityRecord
import io.renku.graph.model.RenkuTinyTypeGenerators.{personGitLabIds, projectSlugs, projectVisibilities}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ProjectSlugRecordsFinderSpec extends AnyWordSpec with should.Matchers with MockFactory with TryValues {

  "apply" should {

    "return SecurityRecords from the TS when found" in new TestCase {

      val securityRecords = securityRecordsGen.generateList(min = 1)
      givenTSSlugRecordsFinder(returning = securityRecords.pure[Try])

      recordsFinder(projectSlug, maybeAuthUser).success.value shouldBe securityRecords
    }

    "return SecurityRecords from the GL when no records found in TS" in new TestCase {

      givenTSSlugRecordsFinder(returning = Nil.pure[Try])

      val securityRecords = securityRecordsGen.generateList(min = 1)
      givenGLSlugRecordsFinder(returning = securityRecords.pure[Try])

      recordsFinder(projectSlug, maybeAuthUser).success.value shouldBe securityRecords
    }
  }

  private trait TestCase {

    val projectSlug   = projectSlugs.generateOne
    val maybeAuthUser = authUsers.generateOption

    private val tsSlugRecordsFinder = mock[TSSlugRecordsFinder[Try]]
    private val glSlugRecordsFinder = mock[GLSlugRecordsFinder[Try]]
    val recordsFinder               = new ProjectSlugRecordsFinderImpl[Try](tsSlugRecordsFinder, glSlugRecordsFinder)

    def givenTSSlugRecordsFinder(returning: Try[List[SecurityRecord]]) =
      (tsSlugRecordsFinder.apply _).expects(projectSlug, maybeAuthUser).returning(returning)

    def givenGLSlugRecordsFinder(returning: Try[List[SecurityRecord]]) =
      (glSlugRecordsFinder.apply _).expects(projectSlug, maybeAuthUser).returning(returning)

    val securityRecordsGen: Gen[SecurityRecord] =
      (projectVisibilities, personGitLabIds.toGeneratorOfSet(min = 0))
        .mapN(SecurityRecord(_, projectSlug, _))
  }
}
