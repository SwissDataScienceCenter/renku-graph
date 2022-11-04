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

package io.renku.graph.model
package entities

import CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import Schemas.{prov, renku, schema}
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLDDecoder.{decodeList, decodeOption}
import io.renku.jsonld._
import io.renku.jsonld.ontology._
import io.renku.jsonld.syntax._
import plans.{Command, DateCreated, DerivedFrom, Description, Keyword, Name, ProgrammingLanguage, ResourceId, SuccessCode}

sealed trait Plan extends Product with Serializable {
  val resourceId:       ResourceId
  val name:             Name
  val maybeDescription: Option[Description]
  val creators:         List[Person]
  val dateCreated:      DateCreated
  val keywords:         List[Keyword]

  type PlanGroup <: Plan
}

object Plan {

  final case class Derivation(derivedFrom: DerivedFrom, originalResourceId: ResourceId)

  implicit def entityFunctions(implicit gitLabApiUrl: GitLabApiUrl): EntityFunctions[Plan] =
    new EntityFunctions[Plan] {
      val findAllPersons: Plan => Set[Person]               = _.creators.toSet
      val encoder:        GraphClass => JsonLDEncoder[Plan] = Plan.encoder(gitLabApiUrl, _)
    }

  implicit def encoder(implicit gitLabApiUrl: GitLabApiUrl, graphClass: GraphClass): JsonLDEncoder[Plan] =
    JsonLDEncoder.instance {
      case p: StepPlan      => p.asJsonLD
      case p: CompositePlan => p.asJsonLD
    }

  implicit def decoder(implicit renkuUrl: RenkuUrl): JsonLDDecoder[Plan] =
    StepPlan.decoder.asInstanceOf[JsonLDDecoder[Plan]]

  lazy val ontology: Type = StepPlan.ontology
}

sealed trait StepPlan extends Plan with StepPlanAlg {
  val maybeCommand:             Option[Command]
  val maybeProgrammingLanguage: Option[ProgrammingLanguage]
  val parameters:               List[CommandParameter]
  val inputs:                   List[CommandInput]
  val outputs:                  List[CommandOutput]
  val successCodes:             List[SuccessCode]
  override type PlanGroup = StepPlan
}

sealed trait StepPlanAlg {
  self: StepPlan =>

  def findParameter(parameterId: commandParameters.ResourceId): Option[CommandParameter] =
    parameters.find(_.resourceId == parameterId)

  def findInput(parameterId: commandParameters.ResourceId): Option[CommandInput] =
    inputs.find(_.resourceId == parameterId)

  def findOutput(parameterId: commandParameters.ResourceId): Option[CommandOutput] =
    outputs.find(_.resourceId == parameterId)
}

object StepPlan {
  import Plan._

  final case class NonModified(resourceId:               ResourceId,
                               name:                     Name,
                               maybeDescription:         Option[Description],
                               creators:                 List[Person],
                               dateCreated:              DateCreated,
                               keywords:                 List[Keyword],
                               maybeCommand:             Option[Command],
                               maybeProgrammingLanguage: Option[ProgrammingLanguage],
                               parameters:               List[CommandParameter],
                               inputs:                   List[CommandInput],
                               outputs:                  List[CommandOutput],
                               successCodes:             List[SuccessCode]
  ) extends StepPlan

  final case class Modified(resourceId:               ResourceId,
                            name:                     Name,
                            maybeDescription:         Option[Description],
                            creators:                 List[Person],
                            dateCreated:              DateCreated,
                            keywords:                 List[Keyword],
                            maybeCommand:             Option[Command],
                            maybeProgrammingLanguage: Option[ProgrammingLanguage],
                            parameters:               List[CommandParameter],
                            inputs:                   List[CommandInput],
                            outputs:                  List[CommandOutput],
                            successCodes:             List[SuccessCode],
                            derivation:               Derivation,
                            maybeInvalidationTime:    Option[InvalidationTime]
  ) extends StepPlan

  def from(resourceId:               ResourceId,
           name:                     Name,
           maybeDescription:         Option[Description],
           maybeCommand:             Option[Command],
           creators:                 List[Person],
           dateCreated:              DateCreated,
           maybeProgrammingLanguage: Option[ProgrammingLanguage],
           keywords:                 List[Keyword],
           parameters:               List[CommandParameter],
           inputs:                   List[CommandInput],
           outputs:                  List[CommandOutput],
           successCodes:             List[SuccessCode]
  ): ValidatedNel[String, StepPlan.NonModified] = NonModified(resourceId,
                                                              name,
                                                              maybeDescription,
                                                              creators,
                                                              dateCreated,
                                                              keywords,
                                                              maybeCommand,
                                                              maybeProgrammingLanguage,
                                                              parameters,
                                                              inputs,
                                                              outputs,
                                                              successCodes
  ).validNel

  def from(resourceId:               ResourceId,
           name:                     Name,
           maybeDescription:         Option[Description],
           maybeCommand:             Option[Command],
           creators:                 List[Person],
           dateCreated:              DateCreated,
           maybeProgrammingLanguage: Option[ProgrammingLanguage],
           keywords:                 List[Keyword],
           parameters:               List[CommandParameter],
           inputs:                   List[CommandInput],
           outputs:                  List[CommandOutput],
           successCodes:             List[SuccessCode],
           derivation:               Derivation,
           maybeInvalidationTime:    Option[InvalidationTime]
  ): ValidatedNel[String, StepPlan] = {

    lazy val validateInvalidationTime: ValidatedNel[String, Unit] = maybeInvalidationTime match {
      case None => Validated.validNel(())
      case Some(time) =>
        Validated.condNel(
          test = (time.value compareTo dateCreated.value) >= 0,
          (),
          show"Invalidation time $time on StepPlan $resourceId is older than dateCreated $dateCreated"
        )
    }

    validateInvalidationTime.map(_ =>
      Modified(
        resourceId,
        name,
        maybeDescription,
        creators,
        dateCreated,
        keywords,
        maybeCommand,
        maybeProgrammingLanguage,
        parameters,
        inputs,
        outputs,
        successCodes,
        derivation,
        maybeInvalidationTime
      )
    )
  }

  val entityTypes: EntityTypes =
    EntityTypes of (renku / "Plan", prov / "Plan", schema / "Action", schema / "CreativeWork")

  implicit def encoder[P <: StepPlan](implicit gitLabApiUrl: GitLabApiUrl, graphClass: GraphClass): JsonLDEncoder[P] =
    JsonLDEncoder.instance {
      case plan: StepPlan.NonModified =>
        JsonLD.entity(
          plan.resourceId.asEntityId,
          entityTypes,
          schema / "name"                -> plan.name.asJsonLD,
          schema / "description"         -> plan.maybeDescription.asJsonLD,
          renku / "command"              -> plan.maybeCommand.asJsonLD,
          schema / "creator"             -> plan.creators.asJsonLD,
          schema / "dateCreated"         -> plan.dateCreated.asJsonLD,
          schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
          schema / "keywords"            -> plan.keywords.asJsonLD,
          renku / "hasArguments"         -> plan.parameters.asJsonLD,
          renku / "hasInputs"            -> plan.inputs.asJsonLD,
          renku / "hasOutputs"           -> plan.outputs.asJsonLD,
          renku / "topmostDerivedFrom"   -> plan.resourceId.asEntityId.asJsonLD,
          renku / "successCodes"         -> plan.successCodes.asJsonLD
        )
      case plan: StepPlan.Modified =>
        JsonLD.entity(
          plan.resourceId.asEntityId,
          entityTypes,
          schema / "name"                -> plan.name.asJsonLD,
          schema / "description"         -> plan.maybeDescription.asJsonLD,
          renku / "command"              -> plan.maybeCommand.asJsonLD,
          schema / "creator"             -> plan.creators.asJsonLD,
          schema / "dateCreated"         -> plan.dateCreated.asJsonLD,
          schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
          schema / "keywords"            -> plan.keywords.asJsonLD,
          renku / "hasArguments"         -> plan.parameters.asJsonLD,
          renku / "hasInputs"            -> plan.inputs.asJsonLD,
          renku / "hasOutputs"           -> plan.outputs.asJsonLD,
          renku / "successCodes"         -> plan.successCodes.asJsonLD,
          prov / "wasDerivedFrom"        -> plan.derivation.derivedFrom.asJsonLD,
          renku / "topmostDerivedFrom"   -> plan.derivation.originalResourceId.asEntityId.asJsonLD,
          prov / "invalidatedAtTime"     -> plan.maybeInvalidationTime.asJsonLD
        )
    }

  implicit def decoder(implicit renkuUrl: RenkuUrl): JsonLDDecoder[StepPlan] =
    JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
      import io.renku.graph.model.views.StringTinyTypeJsonLDDecoders._
      for {
        resourceId            <- cursor.downEntityId.as[ResourceId]
        name                  <- cursor.downField(schema / "name").as[Name]
        maybeDescription      <- cursor.downField(schema / "description").as[Option[Description]]
        maybeCommand          <- cursor.downField(renku / "command").as[Option[Command]]
        creators              <- cursor.downField(schema / "creator").as[List[Person]]
        dateCreated           <- cursor.downField(schema / "dateCreated").as[DateCreated]
        maybeProgrammingLang  <- cursor.downField(schema / "programmingLanguage").as[Option[ProgrammingLanguage]]
        keywords              <- cursor.downField(schema / "keywords").as[List[Option[Keyword]]].map(_.flatten)
        parameters            <- cursor.downField(renku / "hasArguments").as[List[CommandParameter]]
        inputs                <- cursor.downField(renku / "hasInputs").as[List[CommandInput]]
        outputs               <- cursor.downField(renku / "hasOutputs").as[List[CommandOutput]]
        successCodes          <- cursor.downField(renku / "successCodes").as[List[SuccessCode]]
        maybeDerivedFrom      <- cursor.downField(prov / "wasDerivedFrom").as(decodeOption(DerivedFrom.ttDecoder))
        maybeInvalidationTime <- cursor.downField(prov / "invalidatedAtTime").as[Option[InvalidationTime]]
        plan <- {
                  (maybeDerivedFrom, maybeInvalidationTime) match {
                    case (None, None) =>
                      StepPlan
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
                          successCodes
                        )
                    case (Some(derivedFrom), mit) =>
                      StepPlan
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
                          Derivation(derivedFrom, ResourceId(derivedFrom.value)),
                          mit
                        )
                    case (None, Some(_)) => show"Plan $resourceId has no parent but invalidation time".invalidNel
                  }
                }.toEither.leftMap(errors => DecodingFailure(errors.intercalate("; "), Nil))
      } yield plan
    }

  lazy val ontology: Type = {
    val planClass = Class(renku / "Plan")
    Type.Def(
      planClass,
      ObjectProperties(
        ObjectProperty(renku / "hasArguments", CommandParameterBase.CommandParameter.ontology),
        ObjectProperty(renku / "hasInputs", CommandParameterBase.CommandInput.ontology),
        ObjectProperty(renku / "hasOutputs", CommandParameterBase.CommandOutput.ontology),
        ObjectProperty(schema / "creator", Person.ontology),
        ObjectProperty(prov / "wasDerivedFrom", planClass),
        ObjectProperty(renku / "topmostDerivedFrom", planClass)
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
}

sealed trait CompositePlan extends Plan {
  def plans:    NonEmptyList[ResourceId]
  def mappings: List[ParameterMapping]
  def links:    List[ParameterLink]
}

object CompositePlan {

  final case class NonModified(
      resourceId:       ResourceId,
      name:             Name,
      maybeDescription: Option[Description],
      creators:         List[Person],
      dateCreated:      DateCreated,
      keywords:         List[Keyword],
      plans:            NonEmptyList[ResourceId],
      mappings:         List[ParameterMapping],
      links:            List[ParameterLink]
  ) extends CompositePlan

  final case class Modified(
      resourceId:            ResourceId,
      name:                  Name,
      maybeDescription:      Option[Description],
      creators:              List[Person],
      dateCreated:           DateCreated,
      keywords:              List[Keyword],
      plans:                 NonEmptyList[ResourceId],
      mappings:              List[ParameterMapping],
      links:                 List[ParameterLink],
      maybeInvalidationTime: Option[InvalidationTime],
      derivation:            Plan.Derivation
  ) extends CompositePlan

  val entityTypes: EntityTypes =
    EntityTypes of (renku / "CompositePlan", renku / "Plan", prov / "Plan", schema / "Action", schema / "CreativeWork")

  lazy val ontology: Type = {
    val clazz = Class(renku / "CompositePlan")
    Type.Def(
      clazz,
      ObjectProperties(
        ObjectProperty(schema / "creators", Person.ontology),
        ObjectProperty(prov / "wasDerivedFrom", clazz),
        ObjectProperty(renku / "topmostDerivedFrom", clazz),
        ObjectProperty(renku / "hasSubprocess", Plan.ontology),
        ObjectProperty(renku / "workflowLinks", ParameterLink.ontology),
        ObjectProperty(renku / "hasMappings", ParameterMapping.ontology)
      ),
      DataProperties(
        DataProperty(schema / "name", xsd / "string"),
        DataProperty(schema / "description", xsd / "string"),
        DataProperty(schema / "dateCreated", xsd / "dateTime"),
        DataProperty(schema / "keywords", xsd / "string"),
        DataProperty(prov / "invalidatedAtTime", xsd / "dateTime")
      )
    )
  }

  implicit def encoder(implicit gitLabApiUrl: GitLabApiUrl, graphClass: GraphClass): JsonLDEncoder[CompositePlan] =
    JsonLDEncoder.instance { plan =>
      JsonLD.entity(
        plan.resourceId.asEntityId,
        entityTypes,
        Map(
          schema / "name"         -> plan.name.asJsonLD,
          schema / "description"  -> plan.maybeDescription.asJsonLD,
          schema / "creator"      -> plan.creators.asJsonLD,
          schema / "dateCreated"  -> plan.dateCreated.asJsonLD,
          schema / "keywords"     -> plan.keywords.asJsonLD,
          renku / "hasSubprocess" -> plan.plans.toList.asJsonLD,
          renku / "workflowLinks" -> plan.links.asJsonLD,
          renku / "hasMappings"   -> plan.mappings.asJsonLD
        ) ++ PlanLens.getModifiedProperties
          .get(plan)
          .map { case (derivation, invalidationTime) =>
            Map(
              prov / "wasDerivedFrom"      -> derivation.derivedFrom.asJsonLD,
              renku / "topmostDerivedFrom" -> derivation.originalResourceId.asEntityId.asJsonLD,
              prov / "invalidatedAtTime"   -> invalidationTime.asJsonLD
            )
          }
          .getOrElse(Map(renku / "topmostDerivedFrom" -> plan.resourceId.asEntityId.asJsonLD))
      )
    }
}
