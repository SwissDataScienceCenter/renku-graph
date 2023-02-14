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
import io.renku.cli.model.generators.ActivityGenerators
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import io.renku.jsonld.{JsonLD, JsonLDEncoder, Reverse}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class CliActivitySpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with JsonLDCodecMatchers {

  private implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.generateOne
  private val activityGen = ActivityGenerators.activityGen(Instant.EPOCH)

  "decode/encode" should {
    "be compatible" in {
      forAll(activityGen) { cliActivity =>
        assertCompatibleCodec(cliActivity)
      }
    }

    "work on multiple items" in {
      forAll(activityGen, activityGen) { (act1, act2) =>
        assertCompatibleCodec(act1, act2)
      }
    }

    "fail if there is no Agent entity" in {
      import io.renku.jsonld.syntax._
      val encoder = JsonLDEncoder.instance[CliActivity] { entity =>
        JsonLD.entity(
          entity.resourceId.asEntityId,
          CliActivity.entityTypes,
          Reverse.ofJsonLDsUnsafe(Ontologies.Prov.activity -> entity.generations.asJsonLD),
          Ontologies.Prov.startedAtTime        -> entity.startTime.asJsonLD,
          Ontologies.Prov.endedAtTime          -> entity.endTime.asJsonLD,
          Ontologies.Prov.wasAssociatedWith    -> JsonLD.arr(entity.personAgent.asJsonLD),
          Ontologies.Prov.qualifiedAssociation -> entity.association.asJsonLD,
          Ontologies.Prov.qualifiedUsage       -> entity.usages.asJsonLD,
          Ontologies.Renku.parameter           -> entity.parameters.asJsonLD
        )
      }

      val activity = ActivityGenerators.activityGen(Instant.EPOCH).generateOne

      val Left(error) = activity
        .asFlattenedJsonLD(encoder)
        .cursor
        .as[List[CliActivity]]

      error       shouldBe a[DecodingFailure]
      error.message should endWith(s"Cannot decode SoftwareAgent on activity ${activity.resourceId}")
    }

    "fail if there is no Author entity" in {
      import io.renku.jsonld.syntax._
      val encoder = JsonLDEncoder.instance[CliActivity] { entity =>
        JsonLD.entity(
          entity.resourceId.asEntityId,
          CliActivity.entityTypes,
          Reverse.ofJsonLDsUnsafe(Ontologies.Prov.activity -> entity.generations.asJsonLD),
          Ontologies.Prov.startedAtTime        -> entity.startTime.asJsonLD,
          Ontologies.Prov.endedAtTime          -> entity.endTime.asJsonLD,
          Ontologies.Prov.wasAssociatedWith    -> JsonLD.arr(entity.softwareAgent.asJsonLD),
          Ontologies.Prov.qualifiedAssociation -> entity.association.asJsonLD,
          Ontologies.Prov.qualifiedUsage       -> entity.usages.asJsonLD,
          Ontologies.Renku.parameter           -> entity.parameters.asJsonLD
        )
      }

      val activity = ActivityGenerators.activityGen(Instant.EPOCH).generateOne

      val Left(error) = activity
        .asFlattenedJsonLD(encoder)
        .cursor
        .as[List[CliActivity]]

      error       shouldBe a[DecodingFailure]
      error.message should endWith(s"Cannot decode PersonAgent on activity ${activity.resourceId}")
    }
  }
}
