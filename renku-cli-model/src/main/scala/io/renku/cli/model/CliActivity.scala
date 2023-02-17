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

package io.renku.cli.model

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.Ontologies.{Prov, Renku}
import io.renku.graph.model.activities._
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import monocle.{Lens, Traversal}

final case class CliActivity(
    resourceId:    ResourceId,
    startTime:     StartTime,
    endTime:       EndTime,
    softwareAgent: CliAgent.Software,
    personAgent:   CliAgent.Person,
    association:   CliAssociation,
    usages:        List[CliUsage],
    generations:   List[CliGeneration],
    parameters:    List[CliParameterValue]
) extends CliModel

object CliActivity {

  private[model] val entityTypes: EntityTypes = EntityTypes.of(Prov.Activity)

  implicit def jsonLDDecoder: JsonLDDecoder[CliActivity] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId     <- cursor.downEntityId.as[ResourceId]
        generations    <- cursor.downField(Prov.qualifiedGeneration).as[List[CliGeneration]]
        softwareAgents <- cursor.downField(Prov.wasAssociatedWith).as[List[CliSoftwareAgent]]
        personAgents   <- cursor.downField(Prov.wasAssociatedWith).as[List[CliPerson]]
        softwareAgent <- softwareAgents match {
                           case a :: Nil => CliAgent(a).asRight
                           case _ => DecodingFailure(s"Cannot decode SoftwareAgent on activity $resourceId", Nil).asLeft
                         }
        personAgent <- personAgents match {
                         case a :: Nil => CliAgent(a).asRight
                         case _ => DecodingFailure(s"Cannot decode PersonAgent on activity $resourceId", Nil).asLeft
                       }
        association   <- cursor.downField(Prov.qualifiedAssociation).as[CliAssociation]
        usages        <- cursor.downField(Prov.qualifiedUsage).as[List[CliUsage]]
        startedAtTime <- cursor.downField(Prov.startedAtTime).as[StartTime]
        endedAtTime   <- cursor.downField(Prov.endedAtTime).as[EndTime]
        parameters    <- cursor.downField(Renku.parameter).as[List[CliParameterValue]]
      } yield CliActivity(resourceId,
                          startedAtTime,
                          endedAtTime,
                          softwareAgent,
                          personAgent,
                          association,
                          usages,
                          generations,
                          parameters
      )
    }

  implicit val jsonLDEncoder: JsonLDEncoder[CliActivity] =
    JsonLDEncoder.instance { activity =>
      JsonLD.entity(
        activity.resourceId.asEntityId,
        entityTypes,
        Prov.qualifiedGeneration  -> activity.generations.asJsonLD,
        Prov.wasAssociatedWith    -> JsonLD.arr(activity.softwareAgent.asJsonLD, activity.personAgent.asJsonLD),
        Prov.qualifiedAssociation -> activity.association.asJsonLD,
        Prov.qualifiedUsage       -> activity.usages.asJsonLD,
        Prov.startedAtTime        -> activity.startTime.asJsonLD,
        Prov.endedAtTime          -> activity.endTime.asJsonLD,
        Renku.parameter           -> activity.parameters.asJsonLD
      )
    }

  object Lenses {
    val usages: Lens[CliActivity, List[CliUsage]] =
      Lens[CliActivity, List[CliUsage]](_.usages)(usages => _.copy(usages = usages))

    val generations: Lens[CliActivity, List[CliGeneration]] =
      Lens[CliActivity, List[CliGeneration]](_.generations)(gens => _.copy(generations = gens))

    val usagesEntities: Traversal[CliActivity, CliUsage] =
      usages.composeTraversal(Traversal.fromTraverse[List, CliUsage])

    val generationEntities: Traversal[CliActivity, CliGeneration] =
      generations.composeTraversal(Traversal.fromTraverse[List, CliGeneration])

    val usageEntityPaths: Traversal[CliActivity, EntityPath] =
      usagesEntities.composeLens(CliUsage.Lenses.entityPath)

    val generationEntityPaths: Traversal[CliActivity, EntityPath] =
      generationEntities.composeLens(CliGeneration.Lenses.entityPath)
  }
}
