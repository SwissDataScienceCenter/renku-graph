/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.clients

import ch.datascience.clients.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.tinytypes.Sensitive
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks

class AccessTokenSpec extends WordSpec with PropertyChecks {

  "PersonalAccessToken" should {

    "be Sensitive" in {
      personalAccessTokens.generateOne shouldBe a[Sensitive]
    }

    "be instantiatable from a non-blank String" in {
      forAll(nonEmptyStrings()) { value =>
        PersonalAccessToken.from(value).map(_.value) shouldBe Right(value)
      }
    }

    "fail instantiation for a blank String" in {
      val Left(exception) = PersonalAccessToken.from(" ")

      exception shouldBe an[IllegalArgumentException]
    }
  }

  "OAuthAccessToken" should {

    "be Sensitive" in {
      oauthAccessTokens.generateOne shouldBe a[Sensitive]
    }

    "be instantiatable from a non-blank String" in {
      forAll(nonEmptyStrings()) { value =>
        OAuthAccessToken.from(value).map(_.value) shouldBe Right(value)
      }
    }

    "fail instantiation for a blank String" in {
      val Left(exception) = OAuthAccessToken.from(" ")

      exception shouldBe an[IllegalArgumentException]
    }
  }
}
