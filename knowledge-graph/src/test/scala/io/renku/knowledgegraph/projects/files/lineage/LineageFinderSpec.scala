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

package io.renku.knowledgegraph.projects.files.lineage

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.ResourceId
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.knowledgegraph.projects.files.lineage.LineageGenerators._
import io.renku.knowledgegraph.projects.files.lineage.model._
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery
import org.scalamock.scalatest.MockFactory
import org.scalatest.Assertion
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class LineageFinderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ScalaCheckDrivenPropertyChecks
    with should.Matchers {

  import NodeDetailsFinder._

  "find" should {

    "fetch the edges, trim them and find the nodes detail to return the lineage" in new TestCase {
      forAll { lineage: Lineage =>
        val initialEdgesMap = lineages.generateOne.toEdgesMap
        (edgesFinder
          .findEdges(_: projects.Path, _: Option[AuthUser]))
          .expects(projectPath, maybeAuthUser)
          .returning(initialEdgesMap.pure[IO])

        val trimmedEdgesMap = lineage.toEdgesMap
        (edgesTrimmer.trim _)
          .expects(initialEdgesMap, location)
          .returning(trimmedEdgesMap.pure[IO])

        (nodeDetailsFinder
          .findDetails(_: Set[ExecutionInfo], _: projects.Path)(_: (ExecutionInfo, ResourceId) => SparqlQuery))
          .expects(trimmedEdgesMap.keySet, projectPath, activityIdQuery)
          .returning(lineage.processRunNodes.pure[IO])

        val nodesSet = trimmedEdgesMap.view.mapValues { case (s, t) => s ++ t }.values.toSet.flatten
        (nodeDetailsFinder
          .findDetails(_: Set[Node.Location], _: projects.Path)(_: (Node.Location, ResourceId) => SparqlQuery))
          .expects(nodesSet, projectPath, locationQuery)
          .returning(lineage.locationNodes.pure[IO])

        lineageFinder.find(projectPath, location, maybeAuthUser).unsafeRunSync() shouldBe lineage.some
      }
    }

    "return None if not edges are found" in new TestCase {
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning((Map.empty: EdgeMap).pure[IO])

      lineageFinder.find(projectPath, location, maybeAuthUser).unsafeRunSync() shouldBe None
    }

    "return None if the trimming returns None" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(initialEdgesMap.pure[IO])

      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning((Map.empty: EdgeMap).pure[IO])

      lineageFinder.find(projectPath, location, maybeAuthUser).unsafeRunSync() shouldBe None
    }

    "return a Failure if finding edges fails" in new TestCase {

      val exception = exceptions.generateOne
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(exception.raiseError[IO, EdgeMap])

      lineageFinder.find(projectPath, location, maybeAuthUser) shouldBeFailure exception
    }

    "return a Failure if the trimming fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(initialEdgesMap.pure[IO])

      val exception = exceptions.generateOne
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(exception.raiseError[IO, EdgeMap])

      lineageFinder.find(projectPath, location, maybeAuthUser) shouldBeFailure exception
    }

    "return a Failure if finding plan nodes details fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(initialEdgesMap.pure[IO])

      val lineage         = lineages.generateOne
      val trimmedEdgesMap = lineage.toEdgesMap
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(trimmedEdgesMap.pure[IO])

      val exception = exceptions.generateOne
      (nodeDetailsFinder
        .findDetails(_: Set[ExecutionInfo], _: projects.Path)(_: (ExecutionInfo, ResourceId) => SparqlQuery))
        .expects(trimmedEdgesMap.keySet, projectPath, activityIdQuery)
        .returning(exception.raiseError[IO, Set[Node]])

      lineageFinder.find(projectPath, location, maybeAuthUser) shouldBeFailure exception
    }

    "return a Failure if finding entities nodes details fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(initialEdgesMap.pure[IO])

      val lineage         = lineages.generateOne
      val trimmedEdgesMap = lineage.toEdgesMap
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(trimmedEdgesMap.pure[IO])

      (nodeDetailsFinder
        .findDetails(_: Set[ExecutionInfo], _: projects.Path)(_: (ExecutionInfo, ResourceId) => SparqlQuery))
        .expects(trimmedEdgesMap.keySet, projectPath, activityIdQuery)
        .returning(lineage.processRunNodes.pure[IO])

      val nodesSet = trimmedEdgesMap.view
        .mapValues { case (s, t) => s ++ t }
        .values
        .toSet
        .flatten
      val exception = exceptions.generateOne
      (nodeDetailsFinder
        .findDetails(_: Set[Node.Location], _: projects.Path)(_: (Node.Location, ResourceId) => SparqlQuery))
        .expects(nodesSet, projectPath, locationQuery)
        .returning(exception.raiseError[IO, Set[Node]])

      lineageFinder.find(projectPath, location, maybeAuthUser) shouldBeFailure exception
    }

    "return a Failure when Lineage object instantiation fails" in new TestCase {

      val initialEdgesMap = lineages.generateOne.toEdgesMap
      (edgesFinder
        .findEdges(_: projects.Path, _: Option[AuthUser]))
        .expects(projectPath, maybeAuthUser)
        .returning(initialEdgesMap.pure[IO])

      val lineage         = lineages.generateOne
      val trimmedEdgesMap = lineage.toEdgesMap
      (edgesTrimmer.trim _)
        .expects(initialEdgesMap, location)
        .returning(trimmedEdgesMap.pure[IO])

      (nodeDetailsFinder
        .findDetails(_: Set[ExecutionInfo], _: projects.Path)(_: (ExecutionInfo, ResourceId) => SparqlQuery))
        .expects(trimmedEdgesMap.keySet, projectPath, activityIdQuery)
        .returning(Set.empty[Node].pure[IO])

      val nodesSet = trimmedEdgesMap.view
        .mapValues { case (s, t) => s ++ t }
        .values
        .toSet
        .flatten
      (nodeDetailsFinder
        .findDetails(_: Set[Node.Location], _: projects.Path)(_: (Node.Location, ResourceId) => SparqlQuery))
        .expects(nodesSet, projectPath, locationQuery)
        .returning(Set.empty[Node].pure[IO])

      intercept[Exception](lineageFinder.find(projectPath, location, maybeAuthUser).unsafeRunSync())
    }
  }

  private trait TestCase {
    val edgesFinder       = mock[EdgesFinder[IO]]
    val edgesTrimmer      = mock[EdgesTrimmer[IO]]
    val nodeDetailsFinder = mock[NodeDetailsFinder[IO]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val lineageFinder = new LineageFinderImpl[IO](edgesFinder, edgesTrimmer, nodeDetailsFinder)
    val maybeAuthUser = authUsers.generateOption
    val projectPath   = projectPaths.generateOne
    val location      = nodeLocations.generateOne

    implicit class FailureOps(failure: IO[Option[Lineage]]) {

      def shouldBeFailure(expected: Exception): Assertion = {
        val actual = intercept[Exception](failure.unsafeRunSync())

        actual.getCause   shouldBe expected
        actual.getMessage shouldBe s"Finding lineage for '$projectPath' and '$location' failed"

        logger.loggedOnly(Error(s"Finding lineage for '$projectPath' and '$location' failed", expected))
      }
    }
  }
}
