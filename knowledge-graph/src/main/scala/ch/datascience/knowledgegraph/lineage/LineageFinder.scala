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

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.model.projects.Path
import ch.datascience.knowledgegraph.lineage.model.Node.Location
import ch.datascience.knowledgegraph.lineage.model.{Edge, Lineage, Node}
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import io.renku.jsonld.EntityId
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

trait LineageFinder[Interpretation[_]] {
  def find(projectPath: Path, location: Location): Interpretation[Option[Lineage]]
}

class LineageFinderImpl[Interpretation[_]: MonadError[*[_], Throwable]](
    edgesFinder:       EdgesFinder[Interpretation],
    edgesTrimmer:      EdgesTrimmer[Interpretation],
    nodeDetailsFinder: NodeDetailsFinder[Interpretation],
    logger:            Logger[Interpretation]
) extends LineageFinder[Interpretation] {

  import NodeDetailsFinder._
  import edgesFinder._
  import edgesTrimmer._
  import nodeDetailsFinder._

  import scala.util.control.NonFatal

  def find(projectPath: Path, location: Location): Interpretation[Option[Lineage]] =
    findEdges(projectPath) flatMap {
      case edges if edges.isEmpty => Option.empty[Lineage].pure[Interpretation]
      case edges =>
        trim(edges, location) flatMap {
          case trimmedEdges if trimmedEdges.isEmpty => Option.empty[Lineage].pure[Interpretation]
          case trimmedEdges                         => findDetailsAndLineage(trimmedEdges, projectPath)
        }
    } recoverWith loggingError(projectPath, location)

  private def findDetailsAndLineage(edges: Map[EntityId, (Set[Node.Location], Set[Node.Location])], projectPath: Path) =
    for {
      edgesSet         <- edges.toEdgesSet
      runPlansDetails  <- findDetails(edges.keySet, projectPath)
      locationsDetails <- findDetails(edges.toLocationsSet, projectPath)
      lineage          <- Lineage.from[Interpretation](edgesSet, runPlansDetails ++ locationsDetails)
    } yield lineage.some

  private implicit class EdgesOps(edgesAndLocations: Map[EntityId, (Set[Node.Location], Set[Node.Location])]) {

    lazy val toEdgesSet: Interpretation[Set[Edge]] = {
      edgesAndLocations map { case (runPlan, (sources, targets)) =>
        (sources.map(Edge(_, runPlan.toLocation)) ++ targets.map(Edge(runPlan.toLocation, _))).pure[Interpretation]
      }
    }.toList.sequence.map(_.toSet.flatten)

    lazy val toLocationsSet: Set[Location] =
      edgesAndLocations.view.mapValues { case (s, t) => s ++ t }.values.toSet.flatten
  }

  private implicit class EntityIdOps(entityId: EntityId) {
    lazy val toLocation: Node.Location = Node.Location(entityId.value.toString)
  }

  private def loggingError(projectPath: Path,
                           location:    Location
  ): PartialFunction[Throwable, Interpretation[Option[Lineage]]] = { case NonFatal(ex) =>
    val message = s"Finding lineage for '$projectPath' and '$location' failed"
    logger.error(ex)(message)
    new Exception(message, ex).raiseError[Interpretation, Option[Lineage]]
  }
}

object LineageFinder {

  def apply(
      timeRecorder: SparqlQueryTimeRecorder[IO],
      logger:       Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[LineageFinder[IO]] = for {
    lineageEdgesFinder <- EdgesFinder(timeRecorder, logger)
    lineageDataTrimmer <- LineageDataTrimmer()
    nodeDetailsFinder  <- NodeDetailsFinder(timeRecorder, logger)
  } yield new LineageFinderImpl[IO](
    lineageEdgesFinder,
    lineageDataTrimmer,
    nodeDetailsFinder,
    logger
  )
}
