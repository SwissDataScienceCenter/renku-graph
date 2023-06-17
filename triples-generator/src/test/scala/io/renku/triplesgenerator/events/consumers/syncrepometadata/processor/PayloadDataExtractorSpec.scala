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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.CliProject
import io.renku.compression.Zip
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.graph.model.testentities._
import io.renku.jsonld.parser.ParsingFailure
import io.renku.jsonld.syntax._
import org.scalacheck.Arbitrary
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class PayloadDataExtractorSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers with OptionValues {

  it should "unzip the payload, parse it and extract relevant data" in {

    val testProject      = anyRenkuProjectEntities(anyVisibility, creatorGen = cliShapedPersons).generateOne
    val cliProject       = testProject.to[CliProject]
    val cliProjectJsonLD = cliProject.asJsonLD(CliProject.flatJsonLDEncoder)
    val ioPayload        = Zip.zip[IO](cliProjectJsonLD.toJson.noSpaces).map(ZippedEventPayload)

    (ioPayload >>= (extractor.extractPayloadData(testProject.path, _)))
      .asserting(_.value shouldBe DataExtract(testProject.path, testProject.name))
  }

  it should "fail if unzip fails" in {

    val zippedPayload =
      Arbitrary.arbByte.arbitrary.toGeneratorOfList(min = 1).map(_.toArray).generateAs(ZippedEventPayload.apply)

    extractor.extractPayloadData(projectPaths.generateOne, zippedPayload).assertThrows[Exception]
  }

  it should "fail if parsing fails" in {

    val ioPayload = Zip.zip[IO](nonEmptyStrings().generateOne).map(ZippedEventPayload)

    (ioPayload >>= (extractor.extractPayloadData(projectPaths.generateOne, _)))
      .assertThrows[ParsingFailure]
  }

  it should "fail if decoding fails" in {

    val ioPayload = Zip.zip[IO](jsonLDEntities.generateOne.toJson.noSpaces).map(ZippedEventPayload)

    (ioPayload >>= (extractor.extractPayloadData(projectPaths.generateOne, _)))
      .assertThrows[DecodingFailure]
  }

  private lazy val extractor = new PayloadDataExtractorImpl[IO]
}
