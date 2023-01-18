package io.renku.cli.model

import io.renku.cli.model.Ontologies.Schema
import io.renku.graph.model.datasets.{ExternalSameAs, InternalSameAs}
import io.renku.jsonld.syntax.JsonEncoderOps
import io.renku.jsonld.{EntityId, EntityIdEncoder, EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}
import io.renku.tinytypes.{TinyTypeFactory, UrlTinyType, constraints}

final class CliDatasetSameAs private (val value: String) extends AnyVal with UrlTinyType {
  def toInternalSameAs: InternalSameAs =
    InternalSameAs(value)

  def toExternalSameAs: ExternalSameAs =
    ExternalSameAs(value)
}

object CliDatasetSameAs
    extends TinyTypeFactory[CliDatasetSameAs](new CliDatasetSameAs(_))
    with constraints.Url[CliDatasetSameAs] {

  def apply(entityId: EntityId): CliDatasetSameAs =
    CliDatasetSameAs(entityId.toString)

  implicit val entityIdEncoder: EntityIdEncoder[CliDatasetSameAs] =
    EntityIdEncoder.instance { sameAs =>
      EntityId.of(s"$sameAs/${sameAs.value.hashCode}")
    }

  implicit lazy val jsonLdEncoder: JsonLDEncoder[CliDatasetSameAs] = JsonLDEncoder.instance { sameAs =>
    JsonLD.entity(
      sameAs.asEntityId,
      EntityTypes.of(Schema.URL),
      Schema.url -> EntityId.of(sameAs.value).asJsonLD
    )
  }

  implicit val jsonLdDecoder: JsonLDDecoder[CliDatasetSameAs] =
    JsonLDDecoder.entity(EntityTypes.of(Schema.URL)) {
      _.downField(Schema.url).downEntityId.as[EntityId].map(CliDatasetSameAs(_))
    }
}
