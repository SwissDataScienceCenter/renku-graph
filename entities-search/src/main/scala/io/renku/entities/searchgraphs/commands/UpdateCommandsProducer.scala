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

import CommandsCalculator.calculateCommands
import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.entities.Project

private[searchgraphs] trait UpdateCommandsProducer[F[_]] {
  def toUpdateCommands(project: Project)(modelInfos: List[SearchInfo]): F[List[UpdateCommand]]
}

private class UpdateCommandsProducerImpl[F[_]: MonadThrow](searchInfoFetcher: SearchInfoFetcher[F])
    extends UpdateCommandsProducer[F] {

  import searchInfoFetcher._

  def toUpdateCommands(project: Project)(modelInfos: List[SearchInfo]): F[List[UpdateCommand]] =
    fetchTSSearchInfos(project.resourceId).flatMap { tsInfos =>
      val matchedInfos = matchInfosBySameAs(modelInfos, tsInfos)
      matchedInfos
        .map { case (maybeModelInfo, maybeTsInfo) => CalculatorInfoSet(project, maybeModelInfo, maybeTsInfo) }
        .toList
        .map(calculateCommands)
        .sequence
        .map(_.flatten)
    }

  private def matchInfosBySameAs(modelInfos: List[SearchInfo], tsInfos: List[SearchInfo]) = {
    val distinctDatasets = modelInfos.map(_.topmostSameAs).toSet ++ tsInfos.map(_.topmostSameAs)
    distinctDatasets.map(topSameAs =>
      modelInfos.find(_.topmostSameAs == topSameAs) -> tsInfos.find(_.topmostSameAs == topSameAs)
    )
  }
}
