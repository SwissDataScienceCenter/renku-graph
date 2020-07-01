/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.lineage

import cats.data.OptionT
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.knowledgegraph.lineage.LineageGenerators._
import ch.datascience.knowledgegraph.lineage.model.Lineage
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Try}

class LineageFinderSpec extends WordSpec with MockFactory {

  "find" should {

    "return the fetched and curated lineage" in new TestCase {
      val lineage: model.Lineage = lineages.generateOne
      (lineageDataFinder.find _).expects(projectPath).returning(OptionT.some[Try](lineage))
      (lineageDataCurator.curate _).expects(lineage, location).returning(OptionT.some[Try](lineage))

      lineageFinder.find(projectPath, location) shouldBe lineage.some.pure[Try]
    }

    "return None and should not call the curation process if lineage data finder returns None" in new TestCase {
      (lineageDataFinder.find _).expects(projectPath).returning(OptionT.none[Try, Lineage])

      lineageFinder.find(projectPath, location) shouldBe None.pure[Try]
    }

    "return None if the curation returns None" in new TestCase {
      val lineage: model.Lineage = lineages.generateOne
      (lineageDataFinder.find _).expects(projectPath).returning(OptionT.some[Try](lineage))
      (lineageDataCurator.curate _).expects(lineage, location).returning(OptionT.none[Try, Lineage])

      lineageFinder.find(projectPath, location) shouldBe None.pure[Try]
    }

    "return a Failure if the find lineage data fails" in new TestCase {
      val exception = exceptions.generateOne
      (lineageDataFinder.find _).expects(projectPath).returning(OptionT(exception.raiseError[Try, Option[Lineage]]))

      lineageFinder.find(projectPath, location) shouldBe Failure(exception)
    }

    "return a Failure if the curation fails" in new TestCase {
      val lineage: model.Lineage = lineages.generateOne
      val exception = exceptions.generateOne
      (lineageDataFinder.find _).expects(projectPath).returning(OptionT.some[Try](lineage))
      (lineageDataCurator.curate _)
        .expects(lineage, location)
        .returning(OptionT(exception.raiseError[Try, Option[Lineage]]))

      lineageFinder.find(projectPath, location) shouldBe Failure(exception)
    }

  }

  private trait TestCase {
    val lineageDataFinder  = mock[LineageDataFinder[Try]]
    val lineageDataCurator = mock[LineageDataCurator[Try]]
    val lineageFinder      = new LineageFinderImpl[Try](lineageDataFinder, lineageDataCurator)

    val projectPath = projectPaths.generateOne
    val location    = nodeLocations.generateOne
  }
}
