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

package io.renku.eventlog.subscriptions
import io.renku.eventlog.TypeSerializers
import io.renku.events.CategoryName
import io.renku.graph.model.events.LastSyncedDate
import skunk.codec.all.{timestamptz, varchar}
import skunk.{Decoder, Encoder}

import java.time.{OffsetDateTime, ZoneOffset}

trait SubscriptionTypeSerializers extends TypeSerializers {

  val lastSyncedDateDecoder: Decoder[LastSyncedDate] =
    timestamptz.map(timestamp => LastSyncedDate(timestamp.toInstant))
  val lastSyncedDateEncoder: Encoder[LastSyncedDate] =
    timestamptz.values.contramap((b: LastSyncedDate) =>
      OffsetDateTime.ofInstant(b.value, b.value.atOffset(ZoneOffset.UTC).toZonedDateTime.getZone)
    )

  val categoryNameDecoder: Decoder[CategoryName] = varchar.map(CategoryName.apply)
  val categoryNameEncoder: Encoder[CategoryName] = varchar.contramap(_.value)
}
