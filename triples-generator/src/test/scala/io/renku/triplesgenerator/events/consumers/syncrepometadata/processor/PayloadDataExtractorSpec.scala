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
import io.renku.cli.model.CliProject
import io.renku.eventlog.api.EventLogClient.EventPayload
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.triplesgenerator.api.events.Generators.eventPayloads
import org.scalacheck.Arbitrary
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scodec.bits.ByteVector

class PayloadDataExtractorSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with should.Matchers
    with OptionValues
    with ScalaCheckPropertyChecks {

  forAll(anyRenkuProjectEntities(anyVisibility, creatorGen = cliShapedPersons)) { testProject =>
    it should s"unzip the payload, parse it and extract relevant data - project ${testProject.name}" in {

      val cliProject       = testProject.to[CliProject]
      val cliProjectJsonLD = cliProject.asJsonLD(CliProject.flatJsonLDEncoder)
      val ioPayload        = eventPayloads[IO](cliProjectJsonLD.toJson.noSpaces).map(_.generateOne)

      (ioPayload >>=
        (extractor.extractPayloadData(testProject.path, _)))
        .asserting(
          _.value shouldBe DataExtract.Payload(testProject.path,
                                               testProject.name,
                                               testProject.maybeDescription,
                                               testProject.keywords,
                                               testProject.images
          )
        )
    }
  }

  it should "log an info and return None if unzip fails" in {

    logger.reset()

    val zippedPayload =
      Arbitrary.arbByte.arbitrary
        .toGeneratorOfList(min = 1)
        .map(ba => ByteVector(ba.toVector))
        .generateAs(EventPayload.apply)

    extractor.extractPayloadData(projectPaths.generateOne, zippedPayload).asserting(_ shouldBe None) >>
      logger.getMessages(TestLogger.Level.Error).pure[IO].asserting(_.head.show should include("Unzipping"))
  }

  it should "log an error and return None if parsing fails" in {

    logger.reset()

    val ioPayload = eventPayloads[IO](contentGen = nonEmptyStrings()).map(_.generateOne)

    (ioPayload >>= (extractor.extractPayloadData(projectPaths.generateOne, _))).asserting(_ shouldBe None) >>
      logger.getMessages(TestLogger.Level.Error).pure[IO].asserting(_.head.show should include("ParsingFailure"))
  }

  it should "log a warn and return None if decoding fails" in {

    logger.reset()

    val ioPayload = eventPayloads[IO].map(_.generateOne)

    (ioPayload >>= (extractor.extractPayloadData(projectPaths.generateOne, _))).asserting(_ shouldBe None) >>
      logger.getMessages(TestLogger.Level.Warn).pure[IO].asserting(_.head.show should include("DecodingFailure"))
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val extractor = new PayloadDataExtractorImpl[IO]
}
