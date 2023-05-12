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
package projects
package commands

import cats.MonadThrow

private trait CommandsCalculator[F[_]] {
  def calculateCommands(modelInfo: ProjectSearchInfo, maybeTSInfo: Option[ProjectSearchInfo]): F[List[UpdateCommand]]
}

private object CommandsCalculator {
  def apply[F[_]: MonadThrow]: CommandsCalculator[F] = new CommandsCalculatorImpl[F]
}

private class CommandsCalculatorImpl[F[_]: MonadThrow] extends CommandsCalculator[F] {

  override def calculateCommands(modelInfo:   ProjectSearchInfo,
                                 maybeTSInfo: Option[ProjectSearchInfo]
  ): F[List[UpdateCommand]] = ???
}
