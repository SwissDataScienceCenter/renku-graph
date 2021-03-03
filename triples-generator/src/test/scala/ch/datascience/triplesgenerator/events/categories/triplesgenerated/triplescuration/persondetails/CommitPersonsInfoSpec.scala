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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.data.NonEmptyList
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails.PersonDetailsGenerators._
import io.circe.literal.JsonStringContext
import io.circe.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class CommitPersonsInfoSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "commitInfoPersonDecoder" should {
    "decode valid JSON with a valid author or committer" in {
      forAll { commitPersonInfo: CommitPersonsInfo =>
        val committersJson = commitPersonInfo.committers match {
          case NonEmptyList(author, Nil) =>
            val prefix   = if (new Random().nextBoolean()) "committer" else "author"
            val nameKey  = s"${prefix}_name"
            val emailKey = s"${prefix}_email"
            json""" {
              $nameKey:     ${author.name.value},
              $emailKey:    ${author.email.value.asJson}
            }
              """
          case NonEmptyList(author, committer :: _) =>
            json"""{
              "author_name":     ${author.name.value},
              "author_email":    ${author.email.value.asJson},
              "committer_name":  ${committer.name.value},
              "committer_email": ${committer.email.value.asJson}
            }
            """
        }
        val jsonContent = json"""{
          "id":              ${commitPersonInfo.id.value}
        }""" deepMerge committersJson

        jsonContent.as[CommitPersonsInfo] shouldBe Right(commitPersonInfo)
      }

    }

    "fail if there are no author or committer with an email" in {

      val id              = commitIds.generateOne
      val Left(exception) = json"""{
          "id":              ${id.value},
          "author_name":     "",
          "author_email":    ${userEmails.generateOne.value.asJson},
          "committer_name": "",
          "committer_email": ${userEmails.generateOne.value.asJson}
        }""".as[CommitPersonsInfo]

      exception.getMessage() shouldBe s"No valid author and committer on the commit $id"

    }

    "fail if there are no author or committer with a name " in {
      val id              = commitIds.generateOne
      val Left(exception) = json"""{
          "id":              ${id.value},
          "author_name":     ${userNames.generateOne.value},
          "author_email":    "",
          "committer_name": ${userNames.generateOne.value},
          "committer_email": ""
        }""".as[CommitPersonsInfo]

      exception.getMessage() shouldBe s"No valid author and committer on the commit $id"
    }
  }
}
