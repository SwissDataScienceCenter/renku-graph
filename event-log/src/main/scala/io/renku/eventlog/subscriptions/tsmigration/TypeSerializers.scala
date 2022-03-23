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

package io.renku.eventlog.subscriptions.tsmigration

import io.renku.http.server.version.ServiceVersion
import skunk.codec.all.{timestamptz, varchar}
import skunk.{Decoder, Encoder}

import java.time.{OffsetDateTime, ZoneOffset}

private object TypeSerializers extends TypeSerializers

private trait TypeSerializers extends io.renku.eventlog.TypeSerializers {

  val serviceVersionDecoder: Decoder[ServiceVersion] = varchar.map(ServiceVersion.apply)
  val serviceVersionEncoder: Encoder[ServiceVersion] = varchar.values.contramap(_.value)

  val migrationStatusDecoder: Decoder[MigrationStatus] = varchar.map(MigrationStatus.apply)
  val migrationStatusEncoder: Encoder[MigrationStatus] = varchar.values.contramap(_.value)

  val changeDateDecoder: Decoder[ChangeDate] = timestamptz.map(timestamp => ChangeDate(timestamp.toInstant))
  val changeDateEncoder: Encoder[ChangeDate] = timestamptz.values.contramap((b: ChangeDate) =>
    OffsetDateTime.ofInstant(b.value, b.value.atOffset(ZoneOffset.UTC).toZonedDateTime.getZone)
  )

  val migrationMessageDecoder: Decoder[MigrationMessage] = varchar.map(MigrationMessage.apply)
  val migrationMessageEncoder: Encoder[MigrationMessage] = varchar.values.contramap(_.value)
}
