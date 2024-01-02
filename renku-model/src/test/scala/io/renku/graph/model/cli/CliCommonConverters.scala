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

package io.renku.graph.model.cli

import cats.syntax.all._
import io.renku.cli.model._
import io.renku.graph.model.{RenkuUrl, entityModel, generations, testentities}
import io.renku.jsonld.syntax._

trait CliCommonConverters {

  def from(person: testentities.Person): CliPerson = {
    person.maybeGitLabId.map(_ => throw new Exception(s"Cannot convert Person with GitLabId to CliPerson"))
    val resourceId = person.maybeOrcidId
      .map(id => CliPersonResourceId(id.show))
      .getOrElse(CliPersonResourceId(person.resourceId.value))
    CliPerson(resourceId, person.name, person.maybeEmail, person.maybeAffiliation)
  }

  def from(entity: testentities.Entity)(implicit renkuUrl: RenkuUrl): CliEntity = {
    val id = entityModel.ResourceId(entity.asEntityId.show)
    entity match {
      case testentities.Entity.InputEntity(location, checksum) =>
        location match {
          case l: entityModel.Location.File =>
            CliEntity(CliSingleEntity(id, EntityPath(l.value), checksum, generationIds = Nil))
          case l: entityModel.Location.Folder =>
            CliEntity(CliCollectionEntity(id, EntityPath(l.value), checksum, generationIds = Nil))
        }
      case testentities.Entity.OutputEntity(location, checksum, generation) =>
        val genId = generations.ResourceId(generation.asEntityId.show)
        location match {
          case l: entityModel.Location.File =>
            CliEntity(CliSingleEntity(id, EntityPath(l.value), checksum, List(genId)))
          case l: entityModel.Location.Folder =>
            CliEntity(CliCollectionEntity(id, EntityPath(l.value), checksum, List(genId)))
        }
    }
  }
}

object CliCommonConverters extends CliCommonConverters
