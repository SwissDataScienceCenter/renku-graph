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

package io.renku.entities.searchgraphs.datasets
package commands

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.entities.searchgraphs.UpdateCommand
import io.renku.entities.searchgraphs.datasets.ModelDatasetSearchInfo
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private[datasets] trait UpdateCommandsProducer[F[_]] {
  def toUpdateCommands(project: ProjectIdentification)(modelInfos: List[ModelDatasetSearchInfo]): F[List[UpdateCommand]]
}

private[datasets] object UpdateCommandsProducer {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): UpdateCommandsProducer[F] =
    new UpdateCommandsProducerImpl[F](TSSearchInfoFetcher[F](connectionConfig), CommandsCalculator)
}

private class UpdateCommandsProducerImpl[F[_]: MonadThrow](tsInfoFetcher: TSSearchInfoFetcher[F],
                                                           commandsCalculator: CommandsCalculator
) extends UpdateCommandsProducer[F] {

  import tsInfoFetcher.{findTSInfoBySameAs, findTSInfosByProject}

  def toUpdateCommands(
      project: ProjectIdentification
  )(modelInfos: List[ModelDatasetSearchInfo]): F[List[UpdateCommand]] =
    for {
      tsInfosByProject <- findTSInfosByProject(project.resourceId)
      infoSets         <- toInfoSets(project, modelInfos, tsInfosByProject)
    } yield infoSets >>= commandsCalculator.calculateCommands

  private def toInfoSets(project:          ProjectIdentification,
                         modelInfos:       List[ModelDatasetSearchInfo],
                         tsInfosByProject: List[TSDatasetSearchInfo]
  ): F[List[CalculatorInfoSet]] =
    matchInfosBySameAs(modelInfos, tsInfosByProject).toList
      .map {
        case (Some(mi), None) => findTSInfoBySameAs(mi.topmostSameAs).map(mi.some -> _)
        case tuple            => tuple.pure[F]
      }
      .sequence
      .flatMap(_.map(toInfoSet(project)).sequence)

  private def matchInfosBySameAs(modelInfos: List[ModelDatasetSearchInfo], tsInfos: List[TSDatasetSearchInfo]) = {
    val distinctDatasets = modelInfos.map(_.topmostSameAs).toSet ++ tsInfos.map(_.topmostSameAs)
    distinctDatasets.map(topSameAs =>
      modelInfos.find(_.topmostSameAs == topSameAs) -> tsInfos.find(_.topmostSameAs == topSameAs)
    )
  }

  private def toInfoSet(
      project: ProjectIdentification
  ): ((Option[ModelDatasetSearchInfo], Option[TSDatasetSearchInfo])) => F[CalculatorInfoSet] = {
    case (maybeModelInfo, maybeTsInfo) =>
      MonadThrow[F].fromEither {
        CalculatorInfoSet.from(project, maybeModelInfo, maybeTsInfo)
      }
  }
}
