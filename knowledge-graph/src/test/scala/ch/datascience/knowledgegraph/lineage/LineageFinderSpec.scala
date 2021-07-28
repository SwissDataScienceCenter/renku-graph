/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.authUsers
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.ResourceId
import ch.datascience.http.server.security.model.AuthUser
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.knowledgegraph.lineage.LineageGenerators._
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.rdfstore.SparqlQuery
import org.scalamock.scalatest.MockFactory
import org.scalatest.Assertion
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.util.{Failure, Try}

class LineageFinderSpec extends AnyWordSpec with MockFactory with ScalaCheckDrivenPropertyChecks with should.Matchers {

  import NodeDetailsFinder._

  "find" should {

    "fetch the edges, trim them and find the nodes detail to return the lineage" in new TestCase {
      forAll { lineage: Lineage =>
        val initialEdgesMap = lineages.generateOne.toEdgesMap
        (edgesFinder
          .findEdges(_: projects.Path, _: Option[AuthUser]))
          .expects(projectPath, maybeAuthUser)
          .returning(initialEdgesMap.pure[Try])

        val trimmedEdgesMap = lineage.toEdgesMap
        (edgesTrimmer.trim _)
          .expects(initialEdgesMap, location)
          .returning(trimmedEdgesMap.pure[Try])

        (nodeDetailsFinder
          .findDetails(_: Set[RunInfo], _: projects.Path)(_: (RunInfo, ResourceId) => SparqlQuery))
          .expects(trimmedEdgesMap.keySet, projectPath, runIdQuery)
          .returning(lineage.processRunNodes.pure[Try])

        val nodesSet = trimmedEdgesMap.view.mapValues { case (s, t) => s ++ t }.values.toSet.flatten
        (nodeDetailsFinder
          .findDetails(_: Set[Node.Location], _: projects.Path)(_: (Node.Location, ResourceId) => SparqlQuery))
          .expects(nodesSet, projectPath, locationQuery)
          .returning(lineage.locationNodes.pure[Try])

        lineageFinder.find(projectPath, location, maybeAuthUser) shouldBe lineage.some.pure[Try]
      }
    }

    "return None if not edges are found" in new TestCase {
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning((Map.empty: EdgeMap).pure[Try])

      lineageFinder.find(projectPath, location, maybeAuthUser) shouldBe None.pure[Try]
    }

    "return None if the trimming returns None" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(initialEdgesMap.pure[Try])

      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning((Map.empty: EdgeMap).pure[Try])

      lineageFinder.find(projectPath, location, maybeAuthUser) shouldBe None.pure[Try]
    }

    "return a Failure if finding edges fails" in new TestCase {

      val exception = exceptions.generateOne
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(exception.raiseError[Try, EdgeMap])

      lineageFinder.find(projectPath, location, maybeAuthUser) shouldBeFailure exception
    }

    "return a Failure if the trimming fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(initialEdgesMap.pure[Try])

      val exception = exceptions.generateOne
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(exception.raiseError[Try, EdgeMap])

      lineageFinder.find(projectPath, location, maybeAuthUser) shouldBeFailure exception
    }

    "return a Failure if finding plan nodes details fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(initialEdgesMap.pure[Try])

      val lineage         = lineages.generateOne
      val trimmedEdgesMap = lineage.toEdgesMap
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(trimmedEdgesMap.pure[Try])

      val exception = exceptions.generateOne
      (nodeDetailsFinder
        .findDetails(_: Set[RunInfo], _: projects.Path)(_: (RunInfo, ResourceId) => SparqlQuery))
        .expects(trimmedEdgesMap.keySet, projectPath, runIdQuery)
        .returning(exception.raiseError[Try, Set[Node]])

      lineageFinder.find(projectPath, location, maybeAuthUser) shouldBeFailure exception
    }

    "return a Failure if finding entities nodes details fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(initialEdgesMap.pure[Try])

      val lineage         = lineages.generateOne
      val trimmedEdgesMap = lineage.toEdgesMap
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(trimmedEdgesMap.pure[Try])

      (nodeDetailsFinder
        .findDetails(_: Set[RunInfo], _: projects.Path)(_: (RunInfo, ResourceId) => SparqlQuery))
        .expects(trimmedEdgesMap.keySet, projectPath, runIdQuery)
        .returning(lineage.processRunNodes.pure[Try])

      val nodesSet = trimmedEdgesMap.view
        .mapValues { case (s, t) => s ++ t }
        .values
        .toSet
        .flatten
      val exception = exceptions.generateOne
      (nodeDetailsFinder
        .findDetails(_: Set[Node.Location], _: projects.Path)(_: (Node.Location, ResourceId) => SparqlQuery))
        .expects(nodesSet, projectPath, locationQuery)
        .returning(exception.raiseError[Try, Set[Node]])

      lineageFinder.find(projectPath, location, maybeAuthUser) shouldBeFailure exception
    }

    "return a Failure if instantiation of a Lineage object fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(initialEdgesMap.pure[Try])

      val lineage         = lineages.generateOne
      val trimmedEdgesMap = lineage.toEdgesMap
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(
          trimmedEdgesMap
            .pure[Try]
        )

      (nodeDetailsFinder
        .findDetails(_: Set[RunInfo], _: projects.Path)(_: (RunInfo, ResourceId) => SparqlQuery))
        .expects(trimmedEdgesMap.keySet, projectPath, runIdQuery)
        .returning(lineage.processRunNodes.pure[Try])

      val nodesSet = trimmedEdgesMap.view
        .mapValues { case (s, t) => s ++ t }
        .values
        .toSet
        .flatten
      (nodeDetailsFinder
        .findDetails(_: Set[Node.Location], _: projects.Path)(_: (Node.Location, ResourceId) => SparqlQuery))
        .expects(nodesSet, projectPath, locationQuery)
        .returning(Set.empty[Node].pure[Try])

      lineageFinder.find(projectPath, location, maybeAuthUser) shouldBe a[Failure[_]]
    }
  }

  private trait TestCase {
    val edgesFinder       = mock[EdgesFinder[Try]]
    val edgesTrimmer      = mock[EdgesTrimmer[Try]]
    val nodeDetailsFinder = mock[NodeDetailsFinder[Try]]
    val logger            = TestLogger[Try]()
    val lineageFinder     = new LineageFinderImpl[Try](edgesFinder, edgesTrimmer, nodeDetailsFinder, logger)
    val maybeAuthUser     = authUsers.generateOption
    val projectPath       = projectPaths.generateOne
    val location          = nodeLocations.generateOne

    implicit class FailureOps(failure: Try[Option[Lineage]]) {

      def shouldBeFailure(expected: Exception): Assertion = {
        failure shouldBe a[Failure[_]]

        val Failure(actual) = failure

        actual.getCause   shouldBe expected
        actual.getMessage shouldBe s"Finding lineage for '$projectPath' and '$location' failed"

        logger.loggedOnly(Error(s"Finding lineage for '$projectPath' and '$location' failed", expected))
      }
    }
  }
}
