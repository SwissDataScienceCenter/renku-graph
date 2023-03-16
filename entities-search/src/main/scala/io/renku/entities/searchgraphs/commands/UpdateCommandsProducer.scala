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

package io.renku.entities.searchgraphs
package commands

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.graph.model.projects
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private[searchgraphs] trait UpdateCommandsProducer[F[_]] {
  def toUpdateCommands(project: ProjectIdentification)(modelInfos: List[SearchInfo]): F[List[UpdateCommand]]
}

private[searchgraphs] object UpdateCommandsProducer {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): UpdateCommandsProducer[F] = {
    val searchInfoFetcher = SearchInfoFetcher[F](connectionConfig)
    val visibilityFinder  = VisibilityFinder[F](connectionConfig)
    new UpdateCommandsProducerImpl[F](searchInfoFetcher, visibilityFinder, CommandsCalculator[F]())
  }

  def default[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[UpdateCommandsProducer[F]] =
    ProjectsConnectionConfig[F]().map(apply(_))
}

private class UpdateCommandsProducerImpl[F[_]: MonadThrow](searchInfoFetcher: SearchInfoFetcher[F],
                                                           visibilityFinder:   VisibilityFinder[F],
                                                           commandsCalculator: CommandsCalculator[F]
) extends UpdateCommandsProducer[F] {

  import commandsCalculator._
  import searchInfoFetcher._

  def toUpdateCommands(project: ProjectIdentification)(modelInfos: List[SearchInfo]): F[List[UpdateCommand]] = for {
    tsInfos      <- fetchTSSearchInfos(project.resourceId)
    visibilities <- findVisibilities(findDistinctProjects(tsInfos))
    infoSets     <- toInfoSets(project, modelInfos, tsInfos, visibilities)
    updates      <- calculateAllCommands(infoSets)
  } yield updates

  private lazy val findDistinctProjects: List[SearchInfo] => Set[projects.ResourceId] =
    _.flatMap(_.links.map(_.projectId).toList).toSet

  private lazy val findVisibilities: Set[projects.ResourceId] => F[Map[projects.ResourceId, projects.Visibility]] =
    _.toList.map(id => visibilityFinder.findVisibility(id).map(_.map(id -> _))).sequence.map(_.flatten.toMap)

  private def toInfoSets(project:        ProjectIdentification,
                         modelInfos:     List[SearchInfo],
                         tsInfos:        List[SearchInfo],
                         tsVisibilities: Map[projects.ResourceId, projects.Visibility]
  ) = MonadThrow[F].fromEither {
    matchInfosBySameAs(modelInfos, tsInfos)
      .map { case (maybeModelInfo, maybeTsInfo) =>
        CalculatorInfoSet.from(project, maybeModelInfo, maybeTsInfo, tsVisibilities)
      }
      .toList
      .sequence
  }

  private def matchInfosBySameAs(modelInfos: List[SearchInfo], tsInfos: List[SearchInfo]) = {
    val distinctDatasets = modelInfos.map(_.topmostSameAs).toSet ++ tsInfos.map(_.topmostSameAs)
    distinctDatasets.map(topSameAs =>
      modelInfos.find(_.topmostSameAs == topSameAs) -> tsInfos.find(_.topmostSameAs == topSameAs)
    )
  }

  private lazy val calculateAllCommands: List[CalculatorInfoSet] => F[List[UpdateCommand]] =
    _.map(calculateCommands).sequence.map(_.flatten)
}
