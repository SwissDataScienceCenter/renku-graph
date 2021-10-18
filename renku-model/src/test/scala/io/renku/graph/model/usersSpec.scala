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

package io.renku.graph.model

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.users.{Email, ResourceId}
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode
import io.renku.tinytypes.constraints.NonBlank
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EmailSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "Email" should {

    "be a NonBlank" in {
      Email shouldBe a[NonBlank]
    }
  }

  "instantiation" should {

    "be successful for valid emails" in {
      forAll(emailStrings) { email =>
        Email(email).value shouldBe email
      }
    }

    "fail blank emails" in {
      val Left(exception) = Email.from(blankStrings().generateOne)
      exception shouldBe an[IllegalArgumentException]
    }

    Set(
      nonBlankStrings().generateOne.value,
      s"${blankStrings().generateOne}@${nonBlankStrings().generateOne}",
      s"${nonBlankStrings().generateOne}@${blankStrings().generateOne}",
      s"${nonBlankStrings().generateOne}@${nonBlankStrings().generateOne}@${nonBlankStrings().generateOne}"
    ) foreach { invalidEmail =>
      s"fail '$invalidEmail'" in {

        val Left(exception) = Email.from(invalidEmail)

        exception            shouldBe an[IllegalArgumentException]
        exception.getMessage shouldBe s"'$invalidEmail' is not a valid email"
      }
    }
  }

  "extract username" should {

    "return the username part from the email" in {
      forAll { email: Email =>
        email.extractName.value shouldBe email.value.substring(0, email.value.indexOf("@"))
      }
    }
  }

  private implicit lazy val emailStrings: Gen[String] = {
    val firstCharGen    = frequency(6 -> alphaChar, 2 -> numChar, 1 -> Gen.oneOf("!#$%&*+-/=?_~".toList))
    val nonFirstCharGen = frequency(6 -> alphaChar, 2 -> numChar, 1 -> Gen.oneOf("!#$%&*+-/=?_~.".toList))
    val beforeAts = for {
      firstChar  <- firstCharGen
      otherChars <- nonEmptyList(nonFirstCharGen, minElements = 5, maxElements = 10)
    } yield s"$firstChar${otherChars.toList.mkString("")}"

    for {
      beforeAt <- beforeAts
      afterAt  <- nonEmptyStrings()
    } yield s"$beforeAt@$afterAt"
  }
}

class UsersResourceIdSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "apply(GitLabId)" should {

    "generate 'renkuBaseUrl/users/gitLabId' ResourceId" in {
      implicit val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne
      val gitLabId = userGitLabIds.generateOne

      ResourceId(gitLabId).show shouldBe (renkuBaseUrl / "users" / gitLabId).show
    }
  }

  "showAs[RdfResource]" should {

    "wrap the ResourceId in <> if the id doesn't contain email" in {
      forAll(userResourceIds(maybeEmail = None)) { resourceId =>
        resourceId.showAs[RdfResource] shouldBe s"<${resourceId.value}>"
      }
    }

    "encrypt the local part of the email ResourceId and wrap it in <>" in {
      forAll { email: Email =>
        val username   = email.extractName.value
        val resourceId = ResourceId(s"mailto:$email")

        resourceId.showAs[RdfResource] shouldBe s"<${resourceId.value.replace(username, sparqlEncode(username))}>"
      }
    }
  }
}

class GitLabIdSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  import users.GitLabId

  "parse" should {

    "return a GitLabId for valid id in String" in {
      forAll(userGitLabIds) { gitLabId =>
        GitLabId.parse(gitLabId.toString) shouldBe Right(gitLabId)
      }
    }

    "fail for invalid id in String" in {
      val value = nonBlankStrings().generateOne.value

      val Left(exception) = GitLabId.parse(value)

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"$value not a valid GitLabId"
    }
  }
}
