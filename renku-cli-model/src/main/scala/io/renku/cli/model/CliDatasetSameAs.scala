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

import io.renku.cli.model.Ontologies.Schema
import io.renku.graph.model.datasets.{ExternalSameAs, InternalSameAs}
import io.renku.jsonld.syntax.JsonEncoderOps
import io.renku.jsonld._
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
