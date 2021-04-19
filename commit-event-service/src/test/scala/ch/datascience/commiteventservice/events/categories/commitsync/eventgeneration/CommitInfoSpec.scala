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

package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import io.circe.literal._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CommitInfoSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

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
      val authorUsername    = userNames.generateOne
      val committerUsername = userNames.generateOne
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
      val authorEmail    = userEmails.generateOne
      val committerEmail = userEmails.generateOne
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
      val authorUsername    = userNames.generateOne
      val committerUsername = userNames.generateOne
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

      val Left(exception) = json"""{
        "id":              ${commitInfo.id.value},
        "author_name":     ${blankStrings().generateOne},
        "author_email":    ${blankStrings().generateOne},
        "committer_name":  ${userNames.generateOne.value},
        "committer_email": ${userEmails.generateOne.value},
        "message":         ${commitInfo.message.value},
        "committed_date":  ${commitInfo.committedDate.value},
        "parent_ids":      ${commitInfo.parents.map(_.value)}
      }""".as[CommitInfo]

      exception.getMessage() shouldBe "Neither author name nor email"
    }

    "fail if there are blanks for committer username and email" in {
      val commitInfo = commitInfos.generateOne

      val Left(exception) = json"""{
        "id":              ${commitInfo.id.value},
        "author_name":     ${usernames.generateOne.value},
        "author_email":    ${userEmails.generateOne.value},
        "committer_name":  ${blankStrings().generateOne},
        "committer_email": ${blankStrings().generateOne},
        "message":         ${commitInfo.message.value},
        "committed_date":  ${commitInfo.committedDate.value},
        "parent_ids":      ${commitInfo.parents.map(_.value)}
      }""".as[CommitInfo]

      exception.getMessage() shouldBe "Neither committer name nor email"
    }
  }
}
