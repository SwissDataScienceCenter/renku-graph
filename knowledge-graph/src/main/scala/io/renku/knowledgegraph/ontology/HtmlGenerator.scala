/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.effect.{Async, Deferred, Sync}
import cats.syntax.all._
import io.renku.jsonld.JsonLD
import widoco.gui.GuiController

import java.nio.file.{Files, Path}

private trait HtmlGenerator[F[_]] {
  def generationPath: Path
  def generateHtml:   F[Unit]
}

private object HtmlGenerator {

  val generationPath:                       Path = Path.of("ontology")
  private[ontology] val ontologyJsonLDFile: Path = Path.of("ontology.jsonld")
  private[ontology] val ontologyConfFile:   Path = Path.of("ontology.properties")
  private[ontology] val ontologyConfig = Seq(
    "abstract"               -> "Renku Graph Ontology",
    "ontologyTitle"          -> "Renku Graph Ontology",
    "ontologyName"           -> "Renku Graph Ontology",
    "ontologyPrefix"         -> "renku",
    "ontologyNamespaceURI"   -> "https://swissdatasciencecenter.github.io/renku-ontology#",
    "ontologyRevisionNumber" -> "9",
    "authors"                -> "Renku Graph Team",
    "authorsInstitution"     -> "Swiss Data Science Center",
    "authorsInstitutionURI"  -> "https://datascience.ch",
    "licenseName"            -> "Licensed under the Apache License, Version 2.0",
    "licenseURI"             -> "https://www.apache.org/licenses/LICENSE-2.0"
  )

  def apply[F[_]: Async] = new HtmlGeneratorImpl[F](generationPath, OntologyGenerator())

  private[ontology] val generateHtml: (Path, Path, Path) => Unit = { (ontologyFile, ontologyConfFile, generationPath) =>
    GuiController.main(
      List(
        "-ontFile",
        ontologyFile.toString,
        "-outFolder",
        generationPath.toString,
        "-confFile",
        ontologyConfFile.toString,
        "-rewriteAll",
        "-webVowl",
        "-uniteSections",
        "-excludeIntroduction"
      ).toArray
    )
  }
}

private class HtmlGeneratorImpl[F[_]: Async](
    override val generationPath: Path,
    ontologyGenerator:           OntologyGenerator,
    generateHtml:                (Path, Path, Path) => Unit = HtmlGenerator.generateHtml
) extends HtmlGenerator[F] {
  import HtmlGenerator._

  override def generateHtml: F[Unit] = readinessFlag.tryGet >>= {
    case None    => generationProcess >> readinessFlag.get
    case Some(_) => ().pure[F]
  }

  private lazy val readinessFlag: Deferred[F, Unit] = Deferred.unsafe[F, Unit]
  private lazy val generationProcess: F[Unit] =
    Sync[F].delay[Unit](createDirectory()) >>
      Sync[F].delay[Unit](writeToFile(ontologyGenerator.getOntology)) >>
      Sync[F].delay[Unit](writeOntologyProperties()) >>
      Sync[F].delay[Unit](
        generateHtml(generationPath resolve ontologyJsonLDFile, generationPath resolve ontologyConfFile, generationPath)
      ) >>
      readinessFlag.complete(()).void

  private def createDirectory(): Unit = {
    if (!Files.isDirectory(generationPath)) Files.createDirectory(generationPath)
    ()
  }

  private def writeToFile(ontology: JsonLD): Unit = {
    Files.writeString(generationPath resolve ontologyJsonLDFile, ontology.toJson.spaces2)
    ()
  }

  private def writeOntologyProperties(): Unit = {
    Files.writeString(generationPath resolve ontologyConfFile,
                      ontologyConfig.map { case (key, value) => s"$key=$value" }.mkString("\n")
    )
    ()
  }
}
