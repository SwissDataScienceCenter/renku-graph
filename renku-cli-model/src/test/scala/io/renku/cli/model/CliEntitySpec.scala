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

import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import io.renku.cli.model.CliModel._
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.EntityGenerators
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import io.renku.jsonld.JsonLDDecoder
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliEntitySpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with DiffShouldMatcher
    with JsonLDCodecMatchers {

  private implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.generateOne
  private val singleEntityGen     = EntityGenerators.singleEntityGen
  private val collectionEntityGen = EntityGenerators.collectionEntityGen

  "entity decode/encode" should {
    "be compatible" in {
      forAll(singleEntityGen) { cliEntity =>
        assertCompatibleCodec(cliEntity)
      }
    }

    "work on multiple items" in {
      forAll(singleEntityGen, singleEntityGen) { (cliEntity1, cliEntity2) =>
        assertCompatibleCodec(cliEntity1, cliEntity2)
      }
    }

    "work for only a specific generation id" in {
      val entity1 :: entity2 :: _ =
        EntityGenerators.entityGen.suchThat(_.generationIds.nonEmpty).toGeneratorOfList(min = 2).generateOne

      val jsonLD = List(entity1, entity2).asFlattenedJsonLD

      val decoder = JsonLDDecoder.decodeList(CliEntity.jsonLDDecoderForGeneration(entity1.generationIds.head))
      val result  = jsonLD.cursor.as[List[CliEntity]](decoder)
      result.toOption.get shouldMatchTo List(entity1)
    }
  }

  "collection decode/encode" should {
    "be compatible" in {
      forAll(collectionEntityGen) { cliEntity =>
        assertCompatibleCodec(cliEntity)
      }
    }
    "work on multiple items" in {
      forAll(collectionEntityGen, collectionEntityGen) { (cliColl1, cliColl2) =>
        assertCompatibleCodec(cliColl1, cliColl2)
      }
    }
  }
}
