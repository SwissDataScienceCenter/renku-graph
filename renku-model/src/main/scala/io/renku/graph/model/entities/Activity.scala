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

package io.renku.graph.model.entities

import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.RenkuBaseUrl
import io.renku.graph.model.activities.{EndTime, ResourceId, StartTime}
import io.renku.graph.model.entities.ParameterValue.{CommandInputValue, CommandOutputValue}

final case class Activity(resourceId:  ResourceId,
                          startTime:   StartTime,
                          endTime:     EndTime,
                          author:      Person,
                          agent:       Agent,
                          association: Association,
                          usages:      List[Usage],
                          generations: List[Generation],
                          parameters:  List[ParameterValue]
)

object Activity {
  import io.renku.graph.model.GitLabApiUrl
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  val entityTypes: EntityTypes = EntityTypes of (prov / "Activity")

  def from(resourceId:  ResourceId,
           startTime:   StartTime,
           endTime:     EndTime,
           author:      Person,
           agent:       Agent,
           association: Association,
           usages:      List[Usage],
           generations: List[Generation],
           parameters:  List[ParameterValue]
  ): ValidatedNel[String, Activity] = validateState(usages, generations, parameters).map { _ =>
    Activity(
      resourceId,
      startTime,
      endTime,
      author,
      agent,
      association,
      usages,
      generations,
      parameters
    )
  }

  private[Activity] def validateState(usages:      List[Usage],
                                      generations: List[Generation],
                                      parameters:  List[ParameterValue]
  ): ValidatedNel[String, Unit] = List(
    validateUsages(usages, parameters),
    validateGenerations(generations, parameters)
  ).sequence.void

  private[Activity] def validateUsages(usages:     List[Usage],
                                       parameters: List[ParameterValue]
  ): ValidatedNel[String, Unit] = parameters.foldLeft(Validated.validNel[String, Unit](())) {
    case (result, param: CommandInputValue) =>
      result |+| usages
        .find(_.entity.location == param.value)
        .void
        .toValidNel(s"No Usage found for CommandInputValue with ${param.value}")
    case (result, _) => result
  }

  private[Activity] def validateGenerations(generations: List[Generation],
                                            parameters:  List[ParameterValue]
  ): ValidatedNel[String, Unit] = parameters.foldLeft(Validated.validNel[String, Unit](())) {
    case (result, param: CommandOutputValue) =>
      result |+| generations
        .find(_.entity.location == param.value)
        .void
        .toValidNel(s"No Generation found for CommandOutputValue with ${param.value}")
    case (result, _) => result
  }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Activity] =
    JsonLDEncoder.instance { activity =>
      JsonLD.entity(
        activity.resourceId.asEntityId,
        entityTypes,
        Reverse.ofJsonLDsUnsafe((prov / "activity") -> activity.generations.asJsonLD),
        prov / "startedAtTime"        -> activity.startTime.asJsonLD,
        prov / "endedAtTime"          -> activity.endTime.asJsonLD,
        prov / "wasAssociatedWith"    -> JsonLD.arr(activity.agent.asJsonLD, activity.author.asJsonLD),
        prov / "qualifiedAssociation" -> activity.association.asJsonLD,
        prov / "qualifiedUsage"       -> activity.usages.asJsonLD,
        renku / "parameter"           -> activity.parameters.asJsonLD
      )
    }

  implicit def decoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDDecoder[Activity] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      import io.renku.jsonld.JsonLDDecoder.decodeList

      for {
        resourceId    <- cursor.downEntityId.as[ResourceId]
        generations   <- cursor.focusTop.as(decodeList(Generation.decoder(resourceId)))
        startedAtTime <- cursor.downField(prov / "startedAtTime").as[StartTime]
        endedAtTime   <- cursor.downField(prov / "endedAtTime").as[EndTime]
        agent <- cursor.downField(prov / "wasAssociatedWith").as[List[Agent]] >>= {
                   case agent :: Nil => Right(agent)
                   case _ => Left(DecodingFailure(s"Activity $resourceId without or with multiple agents", Nil))
                 }
        author <- cursor.downField(prov / "wasAssociatedWith").as[List[Person]] >>= {
                    case author :: Nil => Right(author)
                    case _ => Left(DecodingFailure(s"Activity $resourceId without or with multiple authors", Nil))
                  }
        association <- cursor.downField(prov / "qualifiedAssociation").as[Association]
        usages      <- cursor.downField(prov / "qualifiedUsage").as[List[Usage]]
        parameters <- cursor
                        .downField(renku / "parameter")
                        .as[List[ParameterValue]](decodeList(ParameterValue.decoder(association.plan)))
        activity <-
          Activity
            .from(resourceId, startedAtTime, endedAtTime, author, agent, association, usages, generations, parameters)
            .leftMap(s => DecodingFailure(s.toList.mkString("; "), Nil))
            .toEither
      } yield activity
    }
}
