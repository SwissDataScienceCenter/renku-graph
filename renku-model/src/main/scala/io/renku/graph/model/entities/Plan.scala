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
import io.renku.graph.model.Schemas._
import io.renku.graph.model._
import io.renku.graph.model.entities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import io.renku.graph.model.plans.{Command, DateCreated, Description, Keyword, Name, ProgrammingLanguage, ResourceId, SuccessCode}
import io.renku.jsonld.JsonLDDecoder
import io.renku.jsonld.JsonLDDecoder.decodeList

final case class Plan(resourceId:               ResourceId,
                      name:                     Name,
                      maybeDescription:         Option[Description],
                      maybeCommand:             Option[Command],
                      creators:                 Set[Person],
                      dateCreated:              DateCreated,
                      maybeProgrammingLanguage: Option[ProgrammingLanguage],
                      keywords:                 List[Keyword],
                      parameters:               List[CommandParameter],
                      inputs:                   List[CommandInput],
                      outputs:                  List[CommandOutput],
                      successCodes:             List[SuccessCode],
                      maybeInvalidationTime:    Option[InvalidationTime]
) extends PlanAlg

sealed trait PlanAlg {
  self: Plan =>

  def findParameter(parameterId: commandParameters.ResourceId): Option[CommandParameter] =
    parameters.find(_.resourceId == parameterId)

  def findInput(parameterId: commandParameters.ResourceId): Option[CommandInput] =
    inputs.find(_.resourceId == parameterId)

  def findOutput(parameterId: commandParameters.ResourceId): Option[CommandOutput] =
    outputs.find(_.resourceId == parameterId)
}

object Plan {
  import io.renku.jsonld.ontology._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  def from(resourceId:               ResourceId,
           name:                     Name,
           maybeDescription:         Option[Description],
           maybeCommand:             Option[Command],
           creators:                 Set[Person],
           dateCreated:              DateCreated,
           maybeProgrammingLanguage: Option[ProgrammingLanguage],
           keywords:                 List[Keyword],
           parameters:               List[CommandParameter],
           inputs:                   List[CommandInput],
           outputs:                  List[CommandOutput],
           successCodes:             List[SuccessCode],
           maybeInvalidationTime:    Option[InvalidationTime]
  ): ValidatedNel[String, Plan] = {
    def validateInvalidation: Option[InvalidationTime] => ValidatedNel[String, Option[InvalidationTime]] = {
      case None => Validated.validNel(Option.empty[InvalidationTime])
      case mit @ Some(invalidationTime) =>
        Validated.condNel(
          test = (dateCreated.value compareTo invalidationTime.value) <= 0,
          mit,
          s"Invalidation time $invalidationTime on StepPlan with id: $resourceId is older than dateCreated $dateCreated"
        )
    }

    validateInvalidation(maybeInvalidationTime).map(mit =>
      Plan(resourceId,
           name,
           maybeDescription,
           maybeCommand,
           creators,
           dateCreated,
           maybeProgrammingLanguage,
           keywords,
           parameters,
           inputs,
           outputs,
           successCodes,
           mit
      )
    )
  }

  private val entityTypes = EntityTypes.of(prov / "Plan", schema / "Action", schema / "CreativeWork")

  implicit def entityFunctions(implicit gitLabApiUrl: GitLabApiUrl): EntityFunctions[Plan] =
    new EntityFunctions[Plan] {
      val findAllPersons: Plan => Set[Person]               = _.creators
      val encoder:        GraphClass => JsonLDEncoder[Plan] = Plan.encoder(gitLabApiUrl, _)
    }

  implicit def encoder(implicit gitLabApiUrl: GitLabApiUrl, graphClass: GraphClass): JsonLDEncoder[Plan] =
    JsonLDEncoder.instance { plan =>
      JsonLD.entity(
        plan.resourceId.asEntityId,
        entityTypes,
        schema / "name"                -> plan.name.asJsonLD,
        schema / "description"         -> plan.maybeDescription.asJsonLD,
        renku / "command"              -> plan.maybeCommand.asJsonLD,
        schema / "creator"             -> plan.creators.toList.asJsonLD,
        schema / "dateCreated"         -> plan.dateCreated.asJsonLD,
        schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
        schema / "keywords"            -> plan.keywords.asJsonLD,
        renku / "hasArguments"         -> plan.parameters.asJsonLD,
        renku / "hasInputs"            -> plan.inputs.asJsonLD,
        renku / "hasOutputs"           -> plan.outputs.asJsonLD,
        renku / "successCodes"         -> plan.successCodes.asJsonLD,
        prov / "invalidatedAtTime"     -> plan.maybeInvalidationTime.asJsonLD
      )
    }

  implicit def decoder(implicit renkuUrl: RenkuUrl): JsonLDDecoder[Plan] = JsonLDDecoder.cacheableEntity(entityTypes) {
    cursor =>
      import io.renku.graph.model.views.StringTinyTypeJsonLDDecoders._
      for {
        resourceId            <- cursor.downEntityId.as[ResourceId]
        name                  <- cursor.downField(schema / "name").as[Name]
        maybeDescription      <- cursor.downField(schema / "description").as[Option[Description]]
        maybeCommand          <- cursor.downField(renku / "command").as[Option[Command]]
        creators              <- cursor.downField(schema / "creator").as[List[Person]].map(_.toSet)
        dateCreated           <- cursor.downField(schema / "dateCreated").as[DateCreated]
        maybeProgrammingLang  <- cursor.downField(schema / "programmingLanguage").as[Option[ProgrammingLanguage]]
        keywords              <- cursor.downField(schema / "keywords").as[List[Option[Keyword]]].map(_.flatten)
        parameters            <- cursor.downField(renku / "hasArguments").as[List[CommandParameter]]
        inputs                <- cursor.downField(renku / "hasInputs").as[List[CommandInput]]
        outputs               <- cursor.downField(renku / "hasOutputs").as[List[CommandOutput]]
        successCodes          <- cursor.downField(renku / "successCodes").as[List[SuccessCode]]
        maybeInvalidationTime <- cursor.downField(prov / "invalidatedAtTime").as[Option[InvalidationTime]]
        plan <- Plan
                  .from(
                    resourceId,
                    name,
                    maybeDescription,
                    maybeCommand,
                    creators,
                    dateCreated,
                    maybeProgrammingLang,
                    keywords,
                    parameters,
                    inputs,
                    outputs,
                    successCodes,
                    maybeInvalidationTime
                  )
                  .toEither
                  .leftMap(errors => DecodingFailure(errors.intercalate("; "), Nil))
      } yield plan
  }

  lazy val ontology: Type = Type.Def(
    Class(prov / "Plan"),
    ObjectProperties(
      ObjectProperty(renku / "hasArguments", CommandParameterBase.CommandParameter.ontology),
      ObjectProperty(renku / "hasInputs", CommandParameterBase.CommandInput.ontology),
      ObjectProperty(renku / "hasOutputs", CommandParameterBase.CommandOutput.ontology),
      ObjectProperty(schema / "creator", Person.ontology)
    ),
    DataProperties(
      DataProperty(schema / "name", xsd / "string"),
      DataProperty(schema / "description", xsd / "string"),
      DataProperty(renku / "command", xsd / "string"),
      DataProperty(schema / "dateCreated", xsd / "dateTime"),
      DataProperty(schema / "programmingLanguage", xsd / "string"),
      DataProperty(schema / "keywords", xsd / "string"),
      DataProperty(renku / "successCodes", xsd / "int"),
      DataProperty(prov / "invalidatedAtTime", xsd / "dateTime")
    )
  )
}
