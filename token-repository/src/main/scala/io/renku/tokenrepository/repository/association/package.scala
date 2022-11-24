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

package io.renku.tokenrepository.repository

import skunk.codec.all._
import skunk.{Decoder, Encoder}

import java.time.{OffsetDateTime, ZoneOffset}

package object association {

  import io.renku.tokenrepository.repository.association.TokenDates._

  private[association] val createdAtDecoder: Decoder[CreatedAt] =
    timestamptz.map(timestamp => CreatedAt(timestamp.toInstant))
  private[association] val createdAtEncoder: Encoder[CreatedAt] = timestamptz.values.contramap((b: CreatedAt) =>
    OffsetDateTime.ofInstant(b.value, b.value.atOffset(ZoneOffset.UTC).toZonedDateTime.getZone)
  )

  private[association] val expiryDateDecoder: Decoder[ExpiryDate] = date.map(date => ExpiryDate(date))
  private[association] val expiryDateEncoder: Encoder[ExpiryDate] = date.values.contramap((b: ExpiryDate) => b.value)
}
