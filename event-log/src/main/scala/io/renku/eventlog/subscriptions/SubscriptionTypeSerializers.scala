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
import doobie.util.{Get, Put}
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.subscriptions.SubscriptionCategory.{CategoryName, LastSyncedDate}

import java.time.Instant

private trait SubscriptionTypeSerializers extends TypeSerializers {

  implicit val lastSyncedDateGet: Get[LastSyncedDate] = Get[Instant].tmap(LastSyncedDate.apply)
  implicit val lastSyncedDatePut: Put[LastSyncedDate] = Put[Instant].contramap(_.value)

  implicit val categoryNameGet: Get[CategoryName] = Get[String].tmap(CategoryName.apply)
  implicit val categoryNamePut: Put[CategoryName] = Put[String].contramap(_.value)
}
