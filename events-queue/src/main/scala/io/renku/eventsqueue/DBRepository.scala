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

package io.renku.eventsqueue

import io.circe.Json
import io.renku.db.SessionResource
import io.renku.db.syntax._
import io.renku.events.CategoryName
import fs2.Stream

private trait DBRepository[F[_]] {
  def insert(category:             CategoryName, payload: Json): CommandDef[F]
  def eventsStream(category:       CategoryName): QueryDef[F, Stream[F, DequeuedEvent]]
  def markUnderProcessing(eventId: Long): CommandDef[F]
  def delete(eventId:              Long): CommandDef[F]
}

private object DBRepository {
  def apply[F[_], DB](implicit sr: SessionResource[F, DB]): DBRepository[F] =
    new DBRepositoryImpl[F, DB]
}

private class DBRepositoryImpl[F[_], DB](implicit sr: SessionResource[F, DB]) extends DBRepository[F] {

  println(sr)
  override def insert(category:             CategoryName, payload: Json): CommandDef[F] = ???
  override def eventsStream(category:       CategoryName): QueryDef[F, Stream[F, DequeuedEvent]] = ???
  override def markUnderProcessing(eventId: Long): CommandDef[F] = ???
  override def delete(eventId:              Long): CommandDef[F] = ???
}
