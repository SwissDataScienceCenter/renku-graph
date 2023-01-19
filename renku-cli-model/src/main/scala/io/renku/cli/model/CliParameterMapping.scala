package io.renku.cli.model

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.CliParameterMapping.MappedParam
import io.renku.cli.model.Ontologies.{Renku, Schema}
import io.renku.graph.model.commandParameters._
import io.renku.jsonld.syntax.JsonEncoderOps
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliParameterMapping(
    resourceId:   ResourceId,
    name:         Name,
    description:  Option[Description],
    prefix:       Option[Prefix],
    position:     Option[Position],
    defaultValue: Option[ParameterDefaultValue],
    mapsTo:       MappedParam
)

object CliParameterMapping {

  sealed trait MappedParam {
    def fold[A](fa: CliCommandInput => A, fb: CliCommandOutput => A, fc: CliCommandParameter => A): A
  }

  object MappedParam {
    final case class Input(value: CliCommandInput) extends MappedParam {
      def fold[A](fa: CliCommandInput => A, fb: CliCommandOutput => A, fc: CliCommandParameter => A): A = fa(value)
    }
    final case class Output(value: CliCommandOutput) extends MappedParam {
      def fold[A](fa: CliCommandInput => A, fb: CliCommandOutput => A, fc: CliCommandParameter => A): A = fb(value)
    }
    final case class Param(value: CliCommandParameter) extends MappedParam {
      def fold[A](fa: CliCommandInput => A, fb: CliCommandOutput => A, fc: CliCommandParameter => A): A = fc(value)
    }

    def apply(input:  CliCommandInput):     MappedParam = Input(input)
    def apply(output: CliCommandOutput):    MappedParam = Output(output)
    def apply(param:  CliCommandParameter): MappedParam = Param(param)

    implicit val jsonLDDecoder: JsonLDDecoder[MappedParam] = {
      val in    = CliCommandInput.jsonLDDecoder.emap(input => MappedParam(input).asRight)
      val out   = CliCommandOutput.jsonLDDecoder.emap(output => MappedParam(output).asRight)
      val param = CliCommandParameter.jsonLDDecoder.emap(param => MappedParam(param).asRight)

      JsonLDDecoder.instance { cursor =>
        val currentEntityTypes = cursor.getEntityTypes
        (currentEntityTypes.map(CliCommandInput.matchingEntityTypes),
         currentEntityTypes.map(CliCommandOutput.matchingEntityTypes),
         currentEntityTypes.map(CliCommandParameter.matchingEntityTypes)
        ).flatMapN {
          case (true, _, _) => in(cursor)
          case (_, true, _) => out(cursor)
          case (_, _, true) => param(cursor)
          case _ => DecodingFailure(s"Invalid entity for decoding mapped parameter: $currentEntityTypes", Nil).asLeft
        }
      }
    }

    implicit val jsonLDEncoder: JsonLDEncoder[MappedParam] =
      JsonLDEncoder.instance { param =>
        param.fold(_.asJsonLD, _.asJsonLD, _.asJsonLD)
      }
  }

  private val entityTypes: EntityTypes = EntityTypes.of(Renku.ParameterMapping, Renku.CommandParameterBase)

  implicit val jsonLDDecoder: JsonLDDecoder[CliParameterMapping] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        position         <- cursor.downField(Renku.position).as[Option[Position]]
        name             <- cursor.downField(Schema.name).as[Name]
        maybeDescription <- cursor.downField(Schema.description).as[Option[Description]]
        maybePrefix      <- cursor.downField(Renku.prefix).as[Option[Prefix]]
        defaultValue     <- cursor.downField(Schema.defaultValue).as[Option[ParameterDefaultValue]]
        mapsTo           <- cursor.downField(Renku.mapsTo).as[MappedParam]
      } yield CliParameterMapping(resourceId, name, maybeDescription, maybePrefix, position, defaultValue, mapsTo)
    }

  implicit val jsonLDEncoder: FlatJsonLDEncoder[CliParameterMapping] =
    FlatJsonLDEncoder.unsafe { param =>
      JsonLD.entity(
        param.resourceId.asEntityId,
        entityTypes,
        Renku.position      -> param.position.asJsonLD,
        Schema.name         -> param.name.asJsonLD,
        Schema.description  -> param.description.asJsonLD,
        Renku.prefix        -> param.prefix.asJsonLD,
        Schema.defaultValue -> param.defaultValue.asJsonLD,
        Renku.mapsTo        -> param.mapsTo.asJsonLD
      )
    }
}
