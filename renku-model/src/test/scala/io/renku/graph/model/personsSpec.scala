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

package io.renku.graph.model

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.persons.{Email, ResourceId}
import io.renku.graph.model.views.RdfResource
import io.renku.tinytypes.constraints.NonBlank
import org.apache.jena.util.URIref
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EmailSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "Email" should {

    "be a NonBlank" in {
      Email shouldBe a[NonBlank[_]]
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

class PersonResourceIdSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {
  private implicit val renkuUrl: RenkuUrl = renkuUrls.generateOne

  "apply(GitLabId)" should {
    "generate 'renkuUrl/persons/gitLabId' ResourceId" in {
      val gitLabId = personGitLabIds.generateOne
      ResourceId(gitLabId).show shouldBe (renkuUrl / "persons" / gitLabId).show
    }
  }

  "apply(OrcidId)" should {
    "generate 'renkuUrl/persons/orcid/xxxx-xxxx-xxxx-xxxx' ResourceId" in {
      val orcid = personOrcidIds.generateOne
      ResourceId(orcid).show shouldBe (renkuUrl / "persons" / "orcid" / orcid.id).show
    }
  }

  "apply(Email)" should {
    "generate 'mailto:email' ResourceId" in {
      val email = personEmails.generateOne
      ResourceId(email).show shouldBe show"mailto:$email"
    }
  }

  "apply(Name)" should {
    "generate 'renkuUrl/persons/name' ResourceId" in {
      val name = personNames.generateOne
      ResourceId(name).show shouldBe (renkuUrl / "persons" / name).show
    }
  }

  "from(String)" should {
    "return ResourceId.GitLabIdBased for strings matching the url" in {
      val resourceId = personGitLabResourceId.generateOne
      ResourceId.from(resourceId.show) shouldBe resourceId.asRight
    }
    "return ResourceId.OrcidIdBased for strings matching the renku orcid based id like 'renkuUrl/persons/orcid/xxxx-xxxx-xxxx-xxxx'" in {
      val resourceId = personOrcidResourceId.generateOne
      ResourceId.from(resourceId.show) shouldBe resourceId.asRight
    }
    "return ResourceId.EmailBased for strings matching the url" in {
      val resourceId = personEmailResourceId.generateOne
      ResourceId.from(resourceId.show) shouldBe resourceId.asRight
    }
    "return ResourceId.NameBased for strings matching the url" in {
      val resourceId = personNameResourceId.generateOne
      ResourceId.from(resourceId.show) shouldBe resourceId.asRight
    }
    "fail for an unrecognised urls" in {
      val resourceId    = httpUrls().generateOne
      val Left(failure) = ResourceId.from(resourceId)

      failure            shouldBe an[IllegalArgumentException]
      failure.getMessage shouldBe s"$resourceId is not a valid ${ResourceId.typeName}"
    }
  }

  "showAs[RdfResource]" should {

    "URI encode and wrap the ResourceId in <> if the id doesn't contain email" in {
      forAll(Gen.oneOf(personGitLabResourceId, personOrcidResourceId, personNameResourceId).widen[persons.ResourceId]) {
        resourceId =>
          resourceId.showAs[RdfResource] shouldBe s"<${URIref.encode(resourceId.show)}>"
      }
    }

    "encrypt the local part of the email ResourceId, URI encode it and wrap in <>" in {
      forAll { email: Email =>
        val username   = email.extractName.value
        val resourceId = ResourceId(email)

        resourceId.showAs[RdfResource] shouldBe s"<${resourceId.value.replace(username, URIref.encode(username))}>"
      }
    }
  }
}

class GitLabIdSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  import persons.GitLabId

  "parse" should {

    "return a GitLabId for valid id in String" in {
      forAll(personGitLabIds) { gitLabId =>
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

class OrcidIdSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  import persons.OrcidId

  "id" should {

    "return the 'xxxx-xxxx-xxxx-xxxx' part of the full 'https://renku/orcid/xxxx-xxxx-xxxx-xxxx'" in {
      val id = Gen.listOfN(4, Gen.listOfN(4, Gen.choose(0, 9)).map(_.mkString(""))).generateOne.mkString("-")
      OrcidId(s"https://orcid.org/$id").id shouldBe id
    }
  }
}
