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

package ch.datascience.graph.model.testentities

import ch.datascience.graph.model.activities.{EndTime, Order, StartTime}
import ch.datascience.graph.model.entityModel._
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.testentities.Activity._
import ch.datascience.graph.model.testentities.Entity.OutputEntity
import ch.datascience.graph.model.{GitLabApiUrl, RenkuBaseUrl, activities, entities, projects}
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints.UUID
import cats.syntax.all._

final case class Activity(id:                  Id,
                          startTime:           StartTime,
                          endTime:             EndTime,
                          author:              Person,
                          agent:               Agent,
                          project:             Project[ForksCount],
                          order:               Order,
                          associationFactory:  Activity => Association,
                          usageFactories:      List[Activity => Usage],
                          generationFactories: List[Activity => Generation],
                          parameterFactories:  List[Activity => ParameterValue]
) {

  lazy val association: Association          = associationFactory(this)
  lazy val runPlan:     RunPlan              = association.runPlan
  lazy val usages:      List[Usage]          = usageFactories.map(_.apply(this))
  lazy val parameters:  List[ParameterValue] = parameterFactories.map(_.apply(this))
  lazy val generations: List[Generation]     = generationFactories.map(_.apply(this))

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
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID {
    def generate: Id = Id(java.util.UUID.randomUUID.toString)

  }

  def apply(id:                  Id,
            startTime:           StartTime,
            author:              Person,
            agent:               Agent,
            project:             Project[ForksCount],
            order:               Order,
            associationFactory:  Activity => Association,
            usageFactories:      List[Activity => Usage] = Nil,
            generationFactories: List[Activity => Generation] = Nil
  ): Activity = new Activity(id,
                             startTime,
                             EndTime(startTime.value),
                             author,
                             agent,
                             project,
                             order,
                             associationFactory,
                             usageFactories,
                             generationFactories,
                             parameterFactories = Nil
  )

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit lazy val toEntitiesActivity: Activity => entities.Activity = activity =>
    entities.Activity(
      activities.ResourceId(activity.asEntityId.show),
      activity.startTime,
      activity.endTime,
      activity.author.to[entities.Person],
      activity.agent.to[entities.Agent],
      projects.ResourceId(activity.project.asEntityId.show),
      activity.order,
      activity.association.to[entities.Association],
      activity.usages.map(_.to[entities.Usage]),
      activity.generations.map(_.to[entities.Generation]),
      activity.parameters.map(_.to[entities.ParameterValue])
    )

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Activity] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entity.asEntityId,
        EntityTypes of (prov / "Activity"),
        Reverse.ofJsonLDsUnsafe((prov / "activity") -> entity.generations.asJsonLD),
        prov / "startedAtTime"        -> entity.startTime.asJsonLD,
        prov / "endedAtTime"          -> entity.endTime.asJsonLD,
        prov / "wasAssociatedWith"    -> JsonLD.arr(entity.agent.asJsonLD, entity.author.asJsonLD),
        prov / "qualifiedAssociation" -> entity.association.asJsonLD,
        prov / "qualifiedUsage"       -> entity.usages.asJsonLD,
        renku / "parameter"           -> entity.parameters.asJsonLD,
        schema / "isPartOf"           -> entity.project.asJsonLD,
        renku / "order"               -> entity.order.asJsonLD
      )
    }

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Activity] =
    EntityIdEncoder.instance(entity => EntityId of renkuBaseUrl / "activities" / entity.id)
}
