/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import ch.datascience.graph.model.events.{CategoryName, LastSyncedDate}
import io.renku.eventlog.TypeSerializers
import skunk.codec.all.{timestamptz, varchar}
import skunk.{Decoder, Encoder}

import java.time.{OffsetDateTime, ZoneId}

trait SubscriptionTypeSerializers extends TypeSerializers {

  val lastSyncedDateGet: Decoder[LastSyncedDate] =
    timestamptz.map(timestamp => LastSyncedDate(timestamp.toInstant))
  val lastSyncedDatePut: Encoder[LastSyncedDate] =
    timestamptz.values.contramap((b: LastSyncedDate) => OffsetDateTime.ofInstant(b.value, ZoneId.systemDefault()))

  val categoryNameGet: Decoder[CategoryName] = varchar.map(CategoryName.apply)
  val categoryNamePut: Encoder[CategoryName] = varchar.contramap(_.value)
}
