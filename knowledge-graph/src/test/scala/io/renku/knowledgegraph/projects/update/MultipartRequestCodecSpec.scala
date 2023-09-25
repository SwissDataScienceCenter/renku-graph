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

package io.renku.knowledgegraph.projects.update

import Generators.projectUpdatesGen
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.countingGen
import io.renku.graph.model.RenkuTinyTypeGenerators.{projectDescriptions, projectKeywords, projectVisibilities}
import io.renku.graph.model.projects
import io.renku.knowledgegraph.projects.images.ImageGenerators
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class MultipartRequestCodecSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with should.Matchers
    with ScalaCheckPropertyChecks {

  private val codec: MultipartRequestCodec[IO] = MultipartRequestCodec[IO]

  forAll(projectUpdatesGen, countingGen) { (updates, cnt) =>
    it should s"decode/encode the multipart request with multiple values #$cnt" in {
      (codec.encode(updates) >>= (MultipartRequestCodec[IO].decode(_))).asserting(_ shouldBe updates)
    }
  }

  it should "decode/encode the multipart request for empty updates" in {

    val updates = ProjectUpdates.empty

    (codec.encode(updates) >>= (MultipartRequestCodec[IO].decode(_))).asserting(_ shouldBe updates)
  }

  it should "decode/encode new value for the description if set" in {

    val updates = ProjectUpdates.empty.copy(newDescription = projectDescriptions.generateSome.some)

    (codec.encode(updates) >>= (MultipartRequestCodec[IO].decode(_))).asserting(_ shouldBe updates)
  }

  it should "decode/encode new value for the description if removed" in {

    val updates = ProjectUpdates.empty.copy(newDescription = Some(None))

    (codec.encode(updates) >>= (MultipartRequestCodec[IO].decode(_))).asserting(_ shouldBe updates)
  }

  it should "decode/encode new value for the image if set" in {

    val updates = ProjectUpdates.empty.copy(newImage = ImageGenerators.images.generateSome.some)

    (codec.encode(updates) >>= (MultipartRequestCodec[IO].decode(_))).asserting(_ shouldBe updates)
  }

  it should "decode/encode new value for the image if removed" in {

    val updates = ProjectUpdates.empty.copy(newImage = Some(None))

    (codec.encode(updates) >>= (MultipartRequestCodec[IO].decode(_))).asserting(_ shouldBe updates)
  }

  it should "decode/encode new value for the keywords if set" in {

    val updates = ProjectUpdates.empty.copy(newKeywords = projectKeywords.generateSet(min = 1).some)

    (codec.encode(updates) >>= (MultipartRequestCodec[IO].decode(_))).asserting(_ shouldBe updates)
  }

  it should "decode/encode new value for the keywords if removed" in {

    val updates = ProjectUpdates.empty.copy(newKeywords = Set.empty[projects.Keyword].some)

    (codec.encode(updates) >>= (MultipartRequestCodec[IO].decode(_))).asserting(_ shouldBe updates)
  }

  it should "decode/encode new value for the visibility if set" in {

    val updates = ProjectUpdates.empty.copy(newVisibility = projectVisibilities.generateSome)

    (codec.encode(updates) >>= (MultipartRequestCodec[IO].decode(_))).asserting(_ shouldBe updates)
  }
}
