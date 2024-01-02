/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.entities.searchgraphs.projects
package commands

import io.renku.entities.searchgraphs.UpdateCommand
import io.renku.entities.searchgraphs.projects.ProjectSearchInfo

private[projects] trait UpdateCommandsProducer {
  def toUpdateCommands(modelInfo: ProjectSearchInfo): List[UpdateCommand]
}

private[projects] object UpdateCommandsProducer {
  def apply(): UpdateCommandsProducer =
    new UpdateCommandsProducerImpl(CommandsCalculator())
}

private class UpdateCommandsProducerImpl(commandsCalculator: CommandsCalculator) extends UpdateCommandsProducer {

  override def toUpdateCommands(modelInfo: ProjectSearchInfo): List[UpdateCommand] =
    commandsCalculator.calculateCommands(modelInfo)
}
