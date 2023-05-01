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

package io.renku.entities.viewings.collector.persons

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.api.events.UserId

private object Generators {

  def generateProjectWithCreatorAndDataset(userId: UserId) =
    anyRenkuProjectEntities
      .map(replaceProjectCreator(generateSomeCreator(userId)))
      .addDataset(datasetEntities(provenanceInternal))
      .generateOne
      .bimap(
        _.to[entities.Dataset[entities.Dataset.Provenance.Internal]],
        _.to[entities.Project]
      )

  def generateProjectWithCreator(userId: UserId) =
    anyProjectEntities
      .map(replaceProjectCreator(generateSomeCreator(userId)))
      .generateOne
      .to[entities.Project]

  private def generateSomeCreator(userId: UserId) =
    userId
      .fold(
        glId => personEntities(maybeGitLabIds = fixed(glId.some)).map(removeOrcidId),
        email => personEntities(withoutGitLabId, maybeEmails = fixed(email.some)).map(removeOrcidId)
      )
      .generateSome
}
