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

package io.renku.graph.model.entities

import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.activities.{EndTime, ResourceId, StartTime}
import io.renku.graph.model.entities.ParameterValue.{CommandInputValue, CommandOutputValue}
import io.renku.graph.model.{GraphClass, RenkuUrl}
import io.renku.jsonld.JsonLDEncoder

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

  implicit def functions(implicit renkuUrl: RenkuUrl, gitLabApiUrl: GitLabApiUrl): EntityFunctions[Activity] =
    new EntityFunctions[Activity] {

      override val findAllPersons: Activity => Set[Person] = { a =>
        Set(a.author) ++ EntityFunctions[Association].findAllPersons(a.association)
      }

      override val encoder: GraphClass => JsonLDEncoder[Activity] = Activity.encoder(renkuUrl, gitLabApiUrl, _)
    }

  import io.renku.graph.model.Schemas.{prov, renku}
  import io.renku.jsonld.ontology._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder, Reverse}

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

  implicit def encoder(implicit
      renkuUrl: RenkuUrl,
      glApiUrl: GitLabApiUrl,
      graph:    GraphClass
  ): JsonLDEncoder[Activity] = JsonLDEncoder.instance { activity =>
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

  implicit def decoder(implicit dependencyLinks: DependencyLinks, renkuUrl: RenkuUrl): JsonLDDecoder[Activity] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      import io.renku.jsonld.JsonLDDecoder.decodeList

      def checkValid(association: Association)(implicit id: ResourceId): StartTime => JsonLDDecoder.Result[Unit] =
        _ => association.asRight.map(_ => ())
//    This code has been temporarily disabled; see https://github.com/SwissDataScienceCenter/renku-graph/issues/1187
//        startTime =>
//          dependencyLinks.findStepPlan(association.planId) match {
//            case Some(plan) if (startTime.value compareTo plan.dateCreated.value) < 0 =>
//              DecodingFailure(show"Activity $id date $startTime is older than plan ${plan.dateCreated}", Nil).asLeft
//            case _ => ().asRight
//          }

      def checkSingle[T](prop: String)(implicit id: ResourceId): List[T] => JsonLDDecoder.Result[T] = {
        case prop :: Nil => Right(prop)
        case _           => Left(DecodingFailure(s"Activity $id without or with multiple ${prop}s", Nil))
      }

      for {
        implicit0(resourceId: ResourceId) <- cursor.downEntityId.as[ResourceId]
        generations                       <- cursor.focusTop.as(decodeList(Generation.decoder(resourceId)))
        agent         <- cursor.downField(prov / "wasAssociatedWith").as[List[Agent]] >>= checkSingle("agent")
        author        <- cursor.downField(prov / "wasAssociatedWith").as[List[Person]] >>= checkSingle("author")
        association   <- cursor.downField(prov / "qualifiedAssociation").as[Association]
        usages        <- cursor.downField(prov / "qualifiedUsage").as[List[Usage]]
        startedAtTime <- cursor.downField(prov / "startedAtTime").as[StartTime] flatTap checkValid(association)
        endedAtTime   <- cursor.downField(prov / "endedAtTime").as[EndTime]
        parameters    <- cursor.downField(renku / "parameter").as(decodeList(ParameterValue.decoder(association)))
        activity <-
          Activity
            .from(resourceId, startedAtTime, endedAtTime, author, agent, association, usages, generations, parameters)
            .leftMap(s => DecodingFailure(s.toList.mkString("; "), Nil))
            .toEither
      } yield activity
    }

  lazy val ontologyClass: Class = Class(prov / "Activity")
  lazy val ontology: Type = Type.Def(
    ontologyClass,
    ObjectProperties(
      ObjectProperty(prov / "wasAssociatedWith", Agent.ontology, Person.Ontology.typeDef),
      ObjectProperty(prov / "qualifiedAssociation", Association.ontology),
      ObjectProperty(prov / "qualifiedUsage", Usage.ontology),
      ObjectProperty(renku / "parameter", ParameterValue.ontology)
    ),
    DataProperties(DataProperty(prov / "startedAtTime", xsd / "dateTime"),
                   DataProperty(prov / "endedAtTime", xsd / "dateTime")
    )
  )
}
