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

package ch.datascience.graph.model.entities

import cats.syntax.all._
import ch.datascience.graph.model.activities.{EndTime, Order, ResourceId, StartTime}
import ch.datascience.graph.model.projects
import io.circe.DecodingFailure

final case class Activity(resourceId:        ResourceId,
                          startTime:         StartTime,
                          endTime:           EndTime,
                          author:            Person,
                          agent:             Agent,
                          projectResourceId: projects.ResourceId,
                          order:             Order,
                          association:       Association,
                          usages:            List[Usage],
                          generations:       List[Generation],
                          parameters:        List[ParameterValue]
)

object Activity {
  import ch.datascience.graph.model.GitLabApiUrl
  import ch.datascience.graph.model.Schemas._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  private val entityTypes: EntityTypes = EntityTypes of (prov / "Activity")

  implicit def encoder(implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Activity] = JsonLDEncoder.instance {
    entity =>
      import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders.{instantTTEncoder, intTTEncoder}

      JsonLD.entity(
        entity.resourceId.asEntityId,
        entityTypes,
        Reverse.ofJsonLDsUnsafe((prov / "activity") -> entity.generations.asJsonLD),
        prov / "startedAtTime"        -> entity.startTime.asJsonLD,
        prov / "endedAtTime"          -> entity.endTime.asJsonLD,
        prov / "wasAssociatedWith"    -> JsonLD.arr(entity.agent.asJsonLD, entity.author.asJsonLD),
        prov / "qualifiedAssociation" -> entity.association.asJsonLD,
        prov / "qualifiedUsage"       -> entity.usages.asJsonLD,
        renku / "parameter"           -> entity.parameters.asJsonLD,
        schema / "isPartOf"           -> entity.projectResourceId.asEntityId.asJsonLD,
        renku / "order"               -> entity.order.asJsonLD
      )
  }

  implicit lazy val decoder: JsonLDDecoder[Activity] = JsonLDDecoder.entity(entityTypes) { cursor =>
    import ch.datascience.graph.model.views.TinyTypeJsonLDDecoders._
    import io.renku.jsonld.JsonLDDecoder.decodeList

    for {
      resourceId <- cursor.downEntityId.as[ResourceId]
      generations <- cursor.top
                       .map(_.cursor.as[List[Generation]].map(_.filter(_.activityResourceId == resourceId)))
                       .getOrElse(Right(List.empty[Generation]))
      startedAtTime <- cursor.downField(prov / "startedAtTime").as[StartTime]
      endedAtTime   <- cursor.downField(prov / "endedAtTime").as[EndTime]
      agent <- cursor.downField(prov / "wasAssociatedWith").as[List[Agent]] >>= {
                 case agent :: Nil => Right(agent)
                 case _            => Left(DecodingFailure("Activity without or with multiple agents", Nil))
               }
      author <- cursor.downField(prov / "wasAssociatedWith").as[List[Person]] >>= {
                  case author :: Nil => Right(author)
                  case _             => Left(DecodingFailure("Activity without or with multiple authors", Nil))
                }
      projectId   <- cursor.downField(schema / "isPartOf").downEntityId.as[projects.ResourceId]
      order       <- cursor.downField(renku / "order").as[Order]
      association <- cursor.downField(prov / "qualifiedAssociation").as[Association]
      usages      <- cursor.downField(prov / "qualifiedUsage").as[List[Usage]]
      parameters <- cursor
                      .downField(renku / "parameter")
                      .as[List[ParameterValue]](decodeList(ParameterValue.decoder(association.runPlan)))
    } yield Activity(resourceId,
                     startedAtTime,
                     endedAtTime,
                     author,
                     agent,
                     projectId,
                     order,
                     association,
                     usages,
                     generations,
                     parameters
    )
  }
}
