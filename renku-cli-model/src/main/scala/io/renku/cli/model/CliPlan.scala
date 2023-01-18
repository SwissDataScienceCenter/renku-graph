package io.renku.cli.model

import io.renku.cli.model.Ontologies.{Prov, Renku, Schema}
import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.plans._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliPlan(
    id:           ResourceId,
    name:         Name,
    description:  Option[Description],
    creators:     List[CliPerson],
    dateCreated:  DateCreated,
    dateModified: Option[DateModified],
    keywords:     List[Keyword],
    command:      Option[Command],
    // language:         Option[ProgrammingLanguage],
    parameters:       List[CliCommandParameter],
    inputs:           List[CliCommandInput],
    outputs:          List[CliCommandOutput],
    successCodes:     List[SuccessCode],
    derivedFrom:      Option[DerivedFrom],
    invalidationTime: Option[InvalidationTime]
)

object CliPlan {

  private val entityTypes: EntityTypes =
    EntityTypes.of(Renku.Plan, Prov.Plan, Schema.Action, Schema.CreativeWork)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  implicit val jsonLDDecoder: JsonLDDecoder[CliPlan] =
    JsonLDDecoder.cacheableEntity(entityTypes, _.getEntityTypes.map(matchingEntityTypes)) { cursor =>
      for {
        resourceId   <- cursor.downEntityId.as[ResourceId]
        name         <- cursor.downField(Schema.name).as[Name]
        description  <- cursor.downField(Schema.description).as[Option[Description]]
        command      <- cursor.downField(Renku.command).as[Option[Command]]
        creators     <- cursor.downField(Schema.creator).as[List[CliPerson]]
        dateCreated  <- cursor.downField(Schema.dateCreated).as[DateCreated]
        dateModified <- cursor.downField(Schema.dateModified).as[Option[DateModified]]
        // maybeProgrammingLang <- cursor.downField(schema / "programmingLanguage").as[Option[ProgrammingLanguage]]
        keywords     <- cursor.downField(Schema.keywords).as[List[Option[Keyword]]].map(_.flatten)
        parameters   <- cursor.downField(Renku.hasArguments).as[List[CliCommandParameter]]
        inputs       <- cursor.downField(Renku.hasInputs).as[List[CliCommandInput]]
        outputs      <- cursor.downField(Renku.hasOutputs).as[List[CliCommandOutput]]
        successCodes <- cursor.downField(Renku.successCodes).as[List[SuccessCode]]
        derivedFrom <-
          cursor.downField(Prov.wasDerivedFrom).as(JsonLDDecoder.decodeOption(DerivedFrom.ttDecoder))
        invalidationTime <- cursor.downField(Prov.invalidatedAtTime).as[Option[InvalidationTime]]
      } yield CliPlan(
        resourceId,
        name,
        description,
        creators,
        dateCreated,
        dateModified,
        keywords,
        command,
        parameters,
        inputs,
        outputs,
        successCodes,
        derivedFrom,
        invalidationTime
      )
    }

  implicit val jsonLDEncoder: JsonLDEncoder[CliPlan] =
    JsonLDEncoder.instance { plan =>
      JsonLD.entity(
        plan.id.asEntityId,
        entityTypes,
        Schema.name            -> plan.name.asJsonLD,
        Schema.description     -> plan.description.asJsonLD,
        Renku.command          -> plan.command.asJsonLD,
        Schema.creator         -> plan.creators.asJsonLD,
        Schema.dateCreated     -> plan.dateCreated.asJsonLD,
        Schema.dateModified    -> plan.dateModified.asJsonLD,
        Schema.keywords        -> plan.keywords.asJsonLD,
        Renku.hasArguments     -> plan.parameters.asJsonLD,
        Renku.hasInputs        -> plan.inputs.asJsonLD,
        Renku.hasOutputs       -> plan.outputs.asJsonLD,
        Renku.successCodes     -> plan.successCodes.asJsonLD,
        Prov.wasDerivedFrom    -> plan.derivedFrom.asJsonLD,
        Prov.invalidatedAtTime -> plan.invalidationTime.asJsonLD
      )
    }
}
