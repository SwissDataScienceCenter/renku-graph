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

package io.renku.cli.model

import io.circe.DecodingFailure
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.ProjectGenerators
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl, projects}
import io.renku.jsonld.{JsonLD, JsonLDDecoder, Property}
import io.renku.jsonld.syntax._
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class CliProjectSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with JsonLDCodecMatchers
    with EitherValues {

  private implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.generateOne
  private val projectGen = ProjectGenerators.projectGen(Instant.EPOCH)

  "decode/encode" should {
    "be compatible" in {
      forAll(projectGen) { cliProject =>
        assertCompatibleCodec(cliProject)
      }
    }

    "work on multiple items" in {
      forAll(projectGen, projectGen) { (cliProject1, cliProject2) =>
        assertCompatibleCodec(cliProject1, cliProject2)
      }
    }

    "return a DecodingFailure when there's a Person entity that cannot be decoded" in {
      val project = projectGen.generateOne

      val invalidJsonLD = project.asFlattenedJsonLD.asArray
        .map(jsons =>
          JsonLD.arr(
            JsonLD.entity(RenkuTinyTypeGenerators.personResourceIds.generateOne.asEntityId,
                          CliPerson.entityTypes,
                          Map.empty[Property, JsonLD]
            ) :: jsons.toList: _*
          )
        )
        .getOrElse(fail("Expected flattened json"))

      val results = invalidJsonLD.cursor.as(JsonLDDecoder.decodeList(CliProject.projectAndPersonDecoder))

      results.left.value shouldBe a[DecodingFailure]
      results.left.value.getMessage() should include(
        s"Finding Person entities for project ${Right(project.name)} failed: "
      )
    }
  }

  "dateModified" should {

    "be the max of Project's dateCreated, Plans' dateModified, Datasets' dateModified and Activities' startTime" in {
      forAll(projectGen) { cliProject =>
        cliProject.dateModified shouldBe projects.DateModified {
          (
            cliProject.dateCreated.value ::
              cliProject.plans.map(
                _.fold(_.dateModified.value, _.dateModified.value, _.dateModified.value, _.dateModified.value)
              ) :::
              cliProject.activities.map(_.startTime.value) :::
              cliProject.datasets.map(_.dateModified.value)
          ).max
        }
      }
    }
  }
}
