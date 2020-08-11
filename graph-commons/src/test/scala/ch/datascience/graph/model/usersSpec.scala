/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.model

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users.{Email, ResourceId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.tinytypes.constraints.NonBlank
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalatest.matchers._
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

  private implicit val emailStrings: Gen[String] = {
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

  import ch.datascience.rdfstore.SparqlValueEncoder._

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
