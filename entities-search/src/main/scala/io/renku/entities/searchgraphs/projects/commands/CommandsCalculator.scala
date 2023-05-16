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

import Encoders._
import UpdateCommand.Insert
import io.renku.triplesstore.client.syntax._

private trait CommandsCalculator {
  def calculateCommands(modelInfo: ProjectSearchInfo, maybeTSInfo: Option[ProjectSearchInfo]): List[UpdateCommand]
}

private object CommandsCalculator {
  def apply(): CommandsCalculator = new CommandsCalculatorImpl
}

private class CommandsCalculatorImpl extends CommandsCalculator {

  override def calculateCommands(modelInfo:   ProjectSearchInfo,
                                 maybeTSInfo: Option[ProjectSearchInfo]
  ): List[UpdateCommand] = maybeTSInfo match {
    case Some(tsInfo) if tsInfo == modelInfo => List.empty[UpdateCommand]
    case Some(_) => UpdateCommand.Query(ProjectInfoDeleteQuery(modelInfo.id)) :: modelInfo.asQuads.map(Insert).toList
    case None    => modelInfo.asQuads.map(Insert).toList
  }
}
