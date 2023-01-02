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

package io.renku.knowledgegraph.ontology

import HtmlGenerator._
import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class HtmlGeneratorSpec extends AnyWordSpec with should.Matchers with MockFactory with IOSpec {

  "generateHtml" should {

    "generate ontology, " +
      "write it to a '{generationPath}/{HtmlGenerator.ontologyJsonLDFile}' file, " +
      "write ontology properties file and " +
      "generate the ontology page" in new TestCase {

        val ontology = jsonLDEntities.generateOne
        (() => ontologyGenerator.getOntology).expects().returns(ontology)

        generateHtml
          .expects(generationPath resolve ontologyJsonLDFile, generationPath resolve ontologyConfFile, generationPath)
          .returning(())

        htmlGenerator.generateHtml.unsafeRunSync() shouldBe ()

        Files
          .readAllLines(generationPath resolve ontologyConfFile)
          .asScala
          .mkString("\n") shouldBe ontologyConfig.map { case (key, value) => s"$key=$value" }.mkString("\n")

        Files
          .readAllLines(generationPath resolve ontologyJsonLDFile)
          .asScala
          .mkString("\n") shouldBe ontology.toJson.spaces2
      }

    "generate the ontology page once" in new TestCase {

      val ontology = jsonLDEntities.generateOne
      (() => ontologyGenerator.getOntology).expects().returns(ontology)

      generateHtml
        .expects(generationPath resolve ontologyJsonLDFile, generationPath resolve ontologyConfFile, generationPath)
        .returning(())

      htmlGenerator.generateHtml.unsafeRunSync() shouldBe ()
      htmlGenerator.generateHtml.unsafeRunSync() shouldBe ()
    }
  }

  private trait TestCase {
    val generationPath    = Files.createTempDirectory("ontologyDir")
    val ontologyGenerator = mock[OntologyGenerator]
    val generateHtml      = mockFunction[Path, Path, Path, Unit]
    val htmlGenerator     = new HtmlGeneratorImpl[IO](generationPath, ontologyGenerator, generateHtml)
  }
}
