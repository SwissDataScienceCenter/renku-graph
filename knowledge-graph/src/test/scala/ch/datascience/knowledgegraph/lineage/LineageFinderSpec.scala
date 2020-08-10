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

import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.ResourceId
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.knowledgegraph.lineage.LineageGenerators._
import ch.datascience.knowledgegraph.lineage.model.{EdgeMap, Lineage, Node}
import ch.datascience.rdfstore.SparqlQuery
import io.renku.jsonld.EntityId
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.util.{Failure, Try}

class LineageFinderSpec extends WordSpec with MockFactory with ScalaCheckDrivenPropertyChecks {

  import NodesDetailsFinder._

  "find" should {

    "fetch the edges, trim them and find the nodes detail to return the lineage" in new TestCase {
      forAll { lineage: Lineage =>
        val initialEdgesMap = lineages.generateOne.toEdgesMap
        (edgesFinder.findEdges _).expects(projectPath).returning(initialEdgesMap.pure[Try])

        val trimmedEdgesMap = lineage.toEdgesMap
        (edgesTrimmer.trim _)
          .expects(initialEdgesMap, location)
          .returning(trimmedEdgesMap.pure[Try])

        (nodesDetailsFinder
          .findDetails(_: Set[EntityId], _: projects.Path)(_: (EntityId, ResourceId) => SparqlQuery))
          .expects(trimmedEdgesMap.keySet, projectPath, runPlanIdQuery)
          .returning(lineage.processRunNodes.pure[Try])

        val nodesSet = trimmedEdgesMap.mapValues { case (s, t) => s ++ t }.values.toSet.flatten
        (nodesDetailsFinder
          .findDetails(_: Set[Node.Location], _: projects.Path)(_: (Node.Location, ResourceId) => SparqlQuery))
          .expects(nodesSet, projectPath, locationQuery)
          .returning(lineage.locationNodes.pure[Try])

        lineageFinder.find(projectPath, location) shouldBe lineage.some.pure[Try]
      }
    }

    "return None if not edges are found" in new TestCase {
      (edgesFinder.findEdges _)
        .expects(projectPath)
        .returning((Map.empty: EdgeMap).pure[Try])

      lineageFinder.find(projectPath, location) shouldBe None.pure[Try]
    }

    "return None if the trimming returns None" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder.findEdges _).expects(projectPath).returning(initialEdgesMap.pure[Try])

      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(Map.empty[EntityId, (Set[Node.Location], Set[Node.Location])].pure[Try])

      lineageFinder.find(projectPath, location) shouldBe None.pure[Try]
    }

    "return a Failure if finding edges fails" in new TestCase {

      val exception = exceptions.generateOne
      (edgesFinder.findEdges _)
        .expects(projectPath)
        .returning(exception.raiseError[Try, EdgeMap])

      lineageFinder.find(projectPath, location) shouldBeFailure exception
    }

    "return a Failure if the trimming fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder.findEdges _).expects(projectPath).returning(initialEdgesMap.pure[Try])

      val exception = exceptions.generateOne
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(exception.raiseError[Try, EdgeMap])

      lineageFinder.find(projectPath, location) shouldBeFailure exception
    }

    "return a Failure if finding runPlan nodes details fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder.findEdges _).expects(projectPath).returning(initialEdgesMap.pure[Try])

      val lineage         = lineages.generateOne
      val trimmedEdgesMap = lineage.toEdgesMap
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(
          trimmedEdgesMap
            .pure[Try]
        )

      val exception = exceptions.generateOne
      (nodesDetailsFinder
        .findDetails(_: Set[EntityId], _: projects.Path)(_: (EntityId, ResourceId) => SparqlQuery))
        .expects(trimmedEdgesMap.keySet, projectPath, runPlanIdQuery)
        .returning(exception.raiseError[Try, Set[Node]])

      lineageFinder.find(projectPath, location) shouldBeFailure exception
    }

    "return a Failure if finding entities nodes details fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder.findEdges _).expects(projectPath).returning(initialEdgesMap.pure[Try])

      val lineage         = lineages.generateOne
      val trimmedEdgesMap = lineage.toEdgesMap
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(
          trimmedEdgesMap
            .pure[Try]
        )

      (nodesDetailsFinder
        .findDetails(_: Set[EntityId], _: projects.Path)(_: (EntityId, ResourceId) => SparqlQuery))
        .expects(trimmedEdgesMap.keySet, projectPath, runPlanIdQuery)
        .returning(lineage.processRunNodes.pure[Try])

      val nodesSet = trimmedEdgesMap
        .mapValues { case (s, t) => s ++ t }
        .values
        .toSet
        .flatten
      val exception = exceptions.generateOne
      (nodesDetailsFinder
        .findDetails(_: Set[Node.Location], _: projects.Path)(_: (Node.Location, ResourceId) => SparqlQuery))
        .expects(nodesSet, projectPath, locationQuery)
        .returning(exception.raiseError[Try, Set[Node]])

      lineageFinder.find(projectPath, location) shouldBeFailure exception
    }

    "return a Failure if instantiation of a Lineage object fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder.findEdges _).expects(projectPath).returning(initialEdgesMap.pure[Try])

      val lineage         = lineages.generateOne
      val trimmedEdgesMap = lineage.toEdgesMap
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(
          trimmedEdgesMap
            .pure[Try]
        )

      (nodesDetailsFinder
        .findDetails(_: Set[EntityId], _: projects.Path)(_: (EntityId, ResourceId) => SparqlQuery))
        .expects(trimmedEdgesMap.keySet, projectPath, runPlanIdQuery)
        .returning(lineage.processRunNodes.pure[Try])

      val nodesSet = trimmedEdgesMap
        .mapValues { case (s, t) => s ++ t }
        .values
        .toSet
        .flatten
      (nodesDetailsFinder
        .findDetails(_: Set[Node.Location], _: projects.Path)(_: (Node.Location, ResourceId) => SparqlQuery))
        .expects(nodesSet, projectPath, locationQuery)
        .returning(Set.empty[Node].pure[Try])

      lineageFinder.find(projectPath, location) shouldBe a[Failure[_]]
    }
  }

  private trait TestCase {
    val edgesFinder        = mock[EdgesFinder[Try]]
    val edgesTrimmer       = mock[EdgesTrimmer[Try]]
    val nodesDetailsFinder = mock[NodesDetailsFinder[Try]]
    val logger             = TestLogger[Try]()
    val lineageFinder      = new LineageFinderImpl[Try](edgesFinder, edgesTrimmer, nodesDetailsFinder, logger)

    val projectPath = projectPaths.generateOne
    val location    = nodeLocations.generateOne

    implicit class FailureOps(failure: Try[Option[Lineage]]) {

      def shouldBeFailure(expected: Exception) = {
        failure shouldBe a[Failure[_]]

        val Failure(actual) = failure

        actual.getCause   shouldBe expected
        actual.getMessage shouldBe s"Finding lineage for '$projectPath' and '$location' failed"

        logger.loggedOnly(Error(s"Finding lineage for '$projectPath' and '$location' failed", expected))
      }
    }
  }
}
