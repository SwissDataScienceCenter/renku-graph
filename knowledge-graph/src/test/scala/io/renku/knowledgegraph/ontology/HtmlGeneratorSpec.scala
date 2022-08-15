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

    "generate ontology, write it to a '{generationPath}/{HtmlGenerator.ontologyJsonLDFile}' file and generate the ontology page" in new TestCase {

      val ontology = jsonLDEntities.generateOne
      (() => ontologyGenerator.getOntology).expects().returns(ontology)

      generateHtml.expects(generationPath resolve ontologyJsonLDFile, generationPath).returning(())

      htmlGenerator.generateHtml.unsafeRunSync() shouldBe ()

      Files
        .readAllLines(generationPath resolve ontologyJsonLDFile)
        .asScala
        .mkString("\n") shouldBe ontology.toJson.spaces2
    }

    "generate the ontology page once" in new TestCase {

      val ontology = jsonLDEntities.generateOne
      (() => ontologyGenerator.getOntology).expects().returns(ontology)

      generateHtml.expects(generationPath resolve ontologyJsonLDFile, generationPath).returning(())

      htmlGenerator.generateHtml.unsafeRunSync() shouldBe ()
      htmlGenerator.generateHtml.unsafeRunSync() shouldBe ()
    }
  }

  private trait TestCase {
    val generationPath    = Files.createTempDirectory("ontologyDir")
    val ontologyGenerator = mock[OntologyGenerator]
    val generateHtml      = mockFunction[Path, Path, Unit]
    val htmlGenerator     = new HtmlGeneratorImpl[IO](generationPath, ontologyGenerator, generateHtml)
  }
}
