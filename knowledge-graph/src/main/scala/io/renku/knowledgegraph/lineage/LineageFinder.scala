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

package io.renku.knowledgegraph.lineage

import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import io.renku.graph.model.projects.Path
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.lineage.model.Node.Location
import io.renku.knowledgegraph.lineage.model._
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

trait LineageFinder[F[_]] {
  def find(projectPath: Path, location: Location, maybeUser: Option[AuthUser]): F[Option[Lineage]]
}

class LineageFinderImpl[F[_]: MonadThrow: Logger](
    edgesFinder:       EdgesFinder[F],
    edgesTrimmer:      EdgesTrimmer[F],
    nodeDetailsFinder: NodeDetailsFinder[F]
) extends LineageFinder[F] {

  import NodeDetailsFinder._
  import edgesFinder._
  import edgesTrimmer._
  import nodeDetailsFinder._

  import scala.util.control.NonFatal

  def find(projectPath: Path, location: Location, maybeUser: Option[AuthUser]): F[Option[Lineage]] =
    findEdges(projectPath, maybeUser) flatMap {
      case edges if edges.isEmpty => Option.empty[Lineage].pure[F]
      case edges =>
        trim(edges, location) flatMap {
          case trimmedEdges if trimmedEdges.isEmpty => Option.empty[Lineage].pure[F]
          case trimmedEdges                         => findDetailsAndLineage(trimmedEdges, projectPath)
        }
    } recoverWith loggingError(projectPath, location)

  private def findDetailsAndLineage(edges: EdgeMap, projectPath: Path) = for {
    edgesSet         <- edges.toEdgesSet
    plansDetails     <- findDetails(edges.keySet, projectPath)
    locationsDetails <- findDetails(edges.toLocationsSet, projectPath)
    lineage          <- Lineage.from[F](edgesSet, plansDetails ++ locationsDetails)
  } yield lineage.some

  private implicit class EdgesOps(edgesAndLocations: EdgeMap) {

    lazy val toEdgesSet: F[Set[Edge]] = {
      edgesAndLocations map { case (runInfo, (sources, targets)) =>
        (sources.map(Edge(_, runInfo.toLocation)) ++ targets.map(Edge(runInfo.toLocation, _))).pure[F]
      }
    }.toList.sequence.map(_.toSet.flatten)

    lazy val toLocationsSet: Set[Location] =
      edgesAndLocations.view.mapValues { case (s, t) => s ++ t }.values.toSet.flatten
  }

  private implicit class RunInfoOps(runInfo: ExecutionInfo) {
    lazy val toLocation: Node.Location = Node.Location(runInfo.entityId.value.toString)
  }

  private def loggingError(projectPath: Path, location: Location): PartialFunction[Throwable, F[Option[Lineage]]] = {
    case NonFatal(ex) =>
      val message = s"Finding lineage for '$projectPath' and '$location' failed"
      Logger[F].error(ex)(message)
      new Exception(message, ex).raiseError[F, Option[Lineage]]
  }
}

object LineageFinder {

  def apply[F[_]: Async: Parallel: Logger](
      timeRecorder: SparqlQueryTimeRecorder[F]
  ): F[LineageFinder[F]] = for {
    lineageEdgesFinder <- EdgesFinder[F](timeRecorder)
    lineageDataTrimmer <- LineageDataTrimmer[F]()
    nodeDetailsFinder  <- NodeDetailsFinder[F](timeRecorder)
  } yield new LineageFinderImpl[F](
    lineageEdgesFinder,
    lineageDataTrimmer,
    nodeDetailsFinder
  )
}
