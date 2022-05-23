/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.testentities

import cats.syntax.all._
import io.renku.graph.model._
import io.renku.graph.model.activities.{EndTime, StartTime}
import io.renku.graph.model.entityModel._
import io.renku.graph.model.testentities.Activity._
import io.renku.graph.model.testentities.Entity.OutputEntity
import io.renku.tinytypes._
import io.renku.tinytypes.constraints.UUID

final case class Activity(id:                  Id,
                          startTime:           StartTime,
                          endTime:             EndTime,
                          author:              Person,
                          agent:               Agent,
                          associationFactory:  Activity => Association,
                          usageFactories:      List[Activity => Usage],
                          generationFactories: List[Activity => Generation],
                          parameterFactories:  List[Activity => ParameterValue]
) {

  val association: Association          = associationFactory(this)
  val plan:        Plan                 = association.plan
  val usages:      List[Usage]          = usageFactories.map(_.apply(this))
  val parameters:  List[ParameterValue] = parameterFactories.map(_.apply(this))
  val generations: List[Generation]     = generationFactories.map(_.apply(this))

  def findEntity(location: Location): Option[Entity] =
    findUsageEntity(location) orElse findGenerationEntity(location)

  def findUsageEntity(location: Location): Option[Entity] =
    usages.find(_.entity.location == location).map(_.entity)

  def findUsagesChecksum(location: Location): Option[Checksum] =
    findUsageEntity(location).map(_.checksum)

  def findGenerationEntity(location: Location): Option[OutputEntity] =
    generations.find(_.entity.location == location).map(_.entity)

  def findGenerationChecksum(location: Location): Option[Checksum] =
    findGenerationEntity(location).map(_.checksum)
}

object Activity {

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id                         extends TinyTypeFactory[Id](new Id(_)) with UUID[Id]

  def apply(id:                  Id,
            startTime:           StartTime,
            author:              Person,
            agent:               Agent,
            associationFactory:  Activity => Association,
            usageFactories:      List[Activity => Usage] = Nil,
            generationFactories: List[Activity => Generation] = Nil
  ): Activity = new Activity(id,
                             startTime,
                             EndTime(startTime.value),
                             author,
                             agent,
                             associationFactory,
                             usageFactories,
                             generationFactories,
                             parameterFactories = Nil
  )

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def toEntitiesActivity(implicit renkuBaseUrl: RenkuBaseUrl): Activity => entities.Activity = activity =>
    entities.Activity(
      activities.ResourceId(activity.asEntityId.show),
      activity.startTime,
      activity.endTime,
      activity.author.to[entities.Person],
      activity.agent.to[entities.Agent],
      activity.association.to[entities.Association],
      activity.usages.map(_.to[entities.Usage]),
      activity.generations.map(_.to[entities.Generation]),
      activity.parameters.map(_.to[entities.ParameterValue])
    )

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Activity] =
    JsonLDEncoder.instance(activity => activity.to[entities.Activity].asJsonLD)

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Activity] =
    EntityIdEncoder.instance(entity => EntityId of renkuBaseUrl / "activities" / entity.id)
}
