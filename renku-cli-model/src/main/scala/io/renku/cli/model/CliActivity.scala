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
import io.renku.cli.model.CliActivity.Agent
import io.renku.cli.model.Ontologies.{Prov, Renku}
import io.renku.graph.model.activities._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliActivity(
    resourceId:  ResourceId,
    startTime:   StartTime,
    endTime:     EndTime,
    agent:       Agent,
    association: CliAssociation,
    usages:      List[CliUsage],
    generations: List[CliGeneration],
    parameters:  List[CliParameterValue]
) extends CliModel

object CliActivity {
  sealed trait Agent {
    def fold[A](fa: CliPerson => A, fb: CliAgent => A): A
  }

  object Agent {
    final case class Person(person: CliPerson) extends Agent {
      def fold[A](fa: CliPerson => A, fb: CliAgent => A): A = fa(person)
    }
    final case class Software(agent: CliAgent) extends Agent {
      def fold[A](fa: CliPerson => A, fb: CliAgent => A): A = fb(agent)
    }

    def apply(person: CliPerson): Agent = Person(person)
    def apply(agent:  CliAgent):  Agent = Software(agent)

    implicit def jsonLDDecoder: JsonLDDecoder[Agent] = {
      val da = CliPerson.jsonLDDecoder.emap(p => Right(Agent(p)))
      val db = CliAgent.jsonLDDecoder.emap(a => Right(Agent(a)))

      JsonLDDecoder.instance { cursor =>
        val currentTypes = cursor.getEntityTypes
        (currentTypes.map(CliPerson.matchingEntityTypes), currentTypes.map(CliAgent.matchingEntityTypes)).flatMapN {
          case (true, _) => da(cursor)
          case (_, true) => db(cursor)
          case _ => DecodingFailure(s"Invalid entity types for decoding activity agent: $entityTypes", Nil).asLeft
        }
      }
    }

    implicit def jsonLDEncoder: JsonLDEncoder[Agent] =
      JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD))
  }

  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Activity)

  implicit def jsonLDDecoder: JsonLDDecoder[CliActivity] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId  <- cursor.downEntityId.as[ResourceId]
        generations <- cursor.downField(Prov.qualifiedGeneration).as[List[CliGeneration]]
        agents      <- cursor.downField(Prov.wasAssociatedWith).as[List[Agent]]
        agent <- agents.headOption.toRight(
                   DecodingFailure(
                     s"No agent found in activity $resourceId",
                     Nil
                   )
                 )
        association   <- cursor.downField(Prov.qualifiedAssociation).as[CliAssociation]
        usages        <- cursor.downField(Prov.qualifiedUsage).as[List[CliUsage]]
        startedAtTime <- cursor.downField(Prov.startedAtTime).as[StartTime]
        endedAtTime   <- cursor.downField(Prov.endedAtTime).as[EndTime]
        parameters    <- cursor.downField(Renku.parameter).as[List[CliParameterValue]]
      } yield CliActivity(resourceId, startedAtTime, endedAtTime, agent, association, usages, generations, parameters)
    }

  implicit val jsonLDEncoder: JsonLDEncoder[CliActivity] =
    JsonLDEncoder.instance { activity =>
      JsonLD.entity(
        activity.resourceId.asEntityId,
        entityTypes,
        Prov.qualifiedGeneration  -> activity.generations.asJsonLD,
        Prov.wasAssociatedWith    -> activity.agent.asJsonLD,
        Prov.qualifiedAssociation -> activity.association.asJsonLD,
        Prov.qualifiedUsage       -> activity.usages.asJsonLD,
        Prov.startedAtTime        -> activity.startTime.asJsonLD,
        Prov.endedAtTime          -> activity.endTime.asJsonLD,
        Renku.parameter           -> activity.parameters.asJsonLD
      )
    }
}
