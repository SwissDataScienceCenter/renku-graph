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

package io.renku.commiteventservice.events.consumers.commitsync.eventgeneration

import io.circe.literal._
import io.renku.commiteventservice.events.consumers.common.Generators._
import io.renku.commiteventservice.events.consumers.common.{Author, CommitInfo, Committer}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CommitInfoSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers with EitherValues {

  "CommitInfo Decoder" should {

    "decode valid JSON to a CommitInfo object" in {
      forAll { commitInfo: CommitInfo =>
        json"""{
          "id":              ${commitInfo.id.value},
          "author_name":     ${commitInfo.author.name.value},
          "author_email":    ${commitInfo.author.emailToJson},
          "committer_name":  ${commitInfo.committer.name.value},
          "committer_email": ${commitInfo.committer.emailToJson},
          "message":         ${commitInfo.message.value},
          "committed_date":  ${commitInfo.committedDate.value},
          "parent_ids":      ${commitInfo.parents.map(_.value)}
        }""".as[CommitInfo] shouldBe Right(commitInfo)
      }
    }

    "decode valid JSON with blank emails to a CommitInfo object" in {
      val commitInfo        = commitInfos.generateOne
      val authorUsername    = personNames.generateOne
      val committerUsername = personNames.generateOne
      json"""{
        "id":              ${commitInfo.id.value},
        "author_name":     ${authorUsername.value},
        "author_email":    ${blankStrings().generateOne},
        "committer_name":  ${committerUsername.value},
        "committer_email": ${blankStrings().generateOne},
        "message":         ${commitInfo.message.value},
        "committed_date":  ${commitInfo.committedDate.value},
        "parent_ids":      ${commitInfo.parents.map(_.value)}
      }""".as[CommitInfo] shouldBe Right(
        commitInfo.copy(
          author = Author.withName(authorUsername),
          committer = Committer.withName(committerUsername)
        )
      )
    }

    "decode valid JSON with blank usernames to a CommitInfo object" in {
      val commitInfo     = commitInfos.generateOne
      val authorEmail    = personEmails.generateOne
      val committerEmail = personEmails.generateOne
      json"""{
        "id":              ${commitInfo.id.value},
        "author_name":     ${blankStrings().generateOne},
        "author_email":    ${authorEmail.value},
        "committer_name":  ${blankStrings().generateOne},
        "committer_email": ${committerEmail.value},
        "message":         ${commitInfo.message.value},
        "committed_date":  ${commitInfo.committedDate.value},
        "parent_ids":      ${commitInfo.parents.map(_.value)}
      }""".as[CommitInfo] shouldBe Right(
        commitInfo.copy(
          author = Author.withEmail(authorEmail),
          committer = Committer.withEmail(committerEmail)
        )
      )
    }

    "decode invalid emails to Nones" in {
      val commitInfo        = commitInfos.generateOne
      val authorUsername    = personNames.generateOne
      val committerUsername = personNames.generateOne
      json"""{
        "id":              ${commitInfo.id.value},
        "author_name":     ${authorUsername.value},
        "author_email":    "author invalid email",
        "committer_name":  ${committerUsername.value},
        "committer_email": "committer invalid email",
        "message":         ${commitInfo.message.value},
        "committed_date":  ${commitInfo.committedDate.value},
        "parent_ids":      ${commitInfo.parents.map(_.value)}
      }""".as[CommitInfo] shouldBe Right(
        commitInfo.copy(
          author = Author.withName(authorUsername),
          committer = Committer.withName(committerUsername)
        )
      )
    }

    "fail if there are blanks for author username and email" in {
      val commitInfo = commitInfos.generateOne

      val result = json"""{
        "id":              ${commitInfo.id.value},
        "author_name":     ${blankStrings().generateOne},
        "author_email":    ${blankStrings().generateOne},
        "committer_name":  ${personNames.generateOne.value},
        "committer_email": ${personEmails.generateOne.value},
        "message":         ${commitInfo.message.value},
        "committed_date":  ${commitInfo.committedDate.value},
        "parent_ids":      ${commitInfo.parents.map(_.value)}
      }""".as[CommitInfo]

      result.left.value.getMessage() should include("Neither author name nor email")
    }

    "fail if there are blanks for committer username and email" in {
      val commitInfo = commitInfos.generateOne

      val result = json"""{
        "id":              ${commitInfo.id.value},
        "author_name":     ${personUsernames.generateOne.value},
        "author_email":    ${personEmails.generateOne.value},
        "committer_name":  ${blankStrings().generateOne},
        "committer_email": ${blankStrings().generateOne},
        "message":         ${commitInfo.message.value},
        "committed_date":  ${commitInfo.committedDate.value},
        "parent_ids":      ${commitInfo.parents.map(_.value)}
      }""".as[CommitInfo]

      result.left.value.getMessage() should include("Neither committer name nor email")
    }
  }
}
