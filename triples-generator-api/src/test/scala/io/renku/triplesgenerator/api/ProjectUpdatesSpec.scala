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

package io.renku.triplesgenerator.api

import Generators._
import io.circe.Json
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.blankStrings
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectUpdatesSpec extends AnyFlatSpec with should.Matchers with ScalaCheckPropertyChecks with EitherValues {

  it should "encode/decode " in {
    forAll(projectUpdatesGen) { values =>
      values.asJson.hcursor.as[ProjectUpdates].value shouldBe values
    }
  }

  it should "description.value = null to be considered as descriptions removals" in {
    val values = projectUpdatesGen.generateOne.copy(newDescription = Some(None))
    values.asJson.deepDropNullValues
      .deepMerge(Json.obj("description" -> Json.obj("value" -> null)))
      .hcursor
      .as[ProjectUpdates]
      .value shouldBe values
  }

  it should "blank descriptions to be considered as description removals" in {
    val values = projectUpdatesGen.generateOne.copy(newDescription = Some(None))
    values.asJson.deepDropNullValues
      .deepMerge(Json.obj("description" -> Json.obj("value" -> blankStrings().generateOne.asJson)))
      .hcursor
      .as[ProjectUpdates]
      .value shouldBe values
  }
}
