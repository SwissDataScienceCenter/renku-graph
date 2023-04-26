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
import io.renku.graph.model.RenkuTinyTypeGenerators.{personGitLabIds, projectPaths, projectVisibilities}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ProjectPathRecordsFinderSpec extends AnyWordSpec with should.Matchers with MockFactory with TryValues {

  "apply" should {

    "return SecurityRecords from the TS when found" in new TestCase {

      val securityRecords = securityRecordsGen.generateList(min = 1)
      givenTSPathRecordsFinder(returning = securityRecords.pure[Try])

      recordsFinder(projectPath, maybeAuthUser).success.value shouldBe securityRecords
    }

    "return SecurityRecords from the GL when no records found in TS" in new TestCase {

      givenTSPathRecordsFinder(returning = Nil.pure[Try])

      val securityRecords = securityRecordsGen.generateList(min = 1)
      givenGLPathRecordsFinder(returning = securityRecords.pure[Try])

      recordsFinder(projectPath, maybeAuthUser).success.value shouldBe securityRecords
    }
  }

  private trait TestCase {

    val projectPath   = projectPaths.generateOne
    val maybeAuthUser = authUsers.generateOption

    private val tsPathRecordsFinder = mock[TSPathRecordsFinder[Try]]
    private val glPathRecordsFinder = mock[GitLabPathRecordsFinder[Try]]
    val recordsFinder               = new ProjectPathRecordsFinderImpl[Try](tsPathRecordsFinder, glPathRecordsFinder)

    def givenTSPathRecordsFinder(returning: Try[List[SecurityRecord]]) =
      (tsPathRecordsFinder.apply _).expects(projectPath, maybeAuthUser).returning(returning)

    def givenGLPathRecordsFinder(returning: Try[List[SecurityRecord]]) =
      (glPathRecordsFinder.apply _).expects(projectPath, maybeAuthUser).returning(returning)

    val securityRecordsGen: Gen[SecurityRecord] =
      (projectVisibilities, personGitLabIds.toGeneratorOfSet(min = 0))
        .mapN(SecurityRecord(_, projectPath, _))
  }
}
