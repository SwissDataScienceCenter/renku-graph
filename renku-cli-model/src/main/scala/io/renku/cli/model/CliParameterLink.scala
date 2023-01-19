package io.renku.cli.model

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.CliParameterLink.Sink
import io.renku.cli.model.Ontologies.Renku
import io.renku.graph.model.parameterLinks._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliParameterLink(
    id:     ResourceId,
    source: CliCommandOutput,
    sinks:  NonEmptyList[Sink]
)

object CliParameterLink {

  sealed trait Sink {
    def fold[A](fa: CliCommandInput => A, fb: CliCommandParameter => A): A
  }

  object Sink {
    final case class Input(value: CliCommandInput) extends Sink {
      def fold[A](fa: CliCommandInput => A, fb: CliCommandParameter => A): A = fa(value)
    }
    final case class Param(value: CliCommandParameter) extends Sink {
      def fold[A](fa: CliCommandInput => A, fb: CliCommandParameter => A): A = fb(value)
    }

    def apply(value: CliCommandInput):     Sink = Input(value)
    def apply(value: CliCommandParameter): Sink = Param(value)

    implicit val jsonLDDecoder: JsonLDDecoder[Sink] = {
      val in    = CliCommandInput.jsonLDDecoder.emap(input => Sink(input).asRight)
      val param = CliCommandParameter.jsonLDDecoder.emap(param => Sink(param).asRight)

      JsonLDDecoder.instance { cursor =>
        val currentEntityTypes = cursor.getEntityTypes
        (currentEntityTypes.map(CliCommandInput.matchingEntityTypes),
         currentEntityTypes.map(CliCommandParameter.matchingEntityTypes)
        ).flatMapN {
          case (true, _) => in(cursor)
          case (_, true) => param(cursor)
          case _ =>
            DecodingFailure(s"Invalid entity for decoding parameter link sink: $currentEntityTypes", Nil).asLeft
        }
      }
    }

    implicit val jsonLDEncoder: JsonLDEncoder[Sink] =
      JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD))
  }

  private val entityTypes: EntityTypes = EntityTypes.of(Renku.ParameterLink)

  implicit val jsonLDDecoder: JsonLDDecoder[CliParameterLink] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        id     <- cursor.downEntityId.as[ResourceId]
        source <- cursor.downField(Renku.linkSource).as[CliCommandOutput]
        sinks  <- cursor.downField(Renku.linkSink).as[NonEmptyList[Sink]]
      } yield CliParameterLink(id, source, sinks)
    }

  implicit val jsonLDEncoder: FlatJsonLDEncoder[CliParameterLink] =
    FlatJsonLDEncoder.unsafe { link =>
      JsonLD.entity(
        link.id.asEntityId,
        entityTypes,
        Renku.linkSource -> link.source.asJsonLD,
        Renku.linkSink   -> link.sinks.asJsonLD
      )
    }
}
