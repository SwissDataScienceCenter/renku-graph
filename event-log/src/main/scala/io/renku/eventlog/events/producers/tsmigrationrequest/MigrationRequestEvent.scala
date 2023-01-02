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

package io.renku.eventlog.events.producers.tsmigrationrequest

import cats.Show
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.events.consumers.subscriptions.SubscriberUrl

private final case class MigrationRequestEvent(subscriberUrl: SubscriberUrl, subscriberVersion: ServiceVersion)

private object MigrationRequestEvent {
  import io.circe.Json
  import io.circe.literal._

  def encodeEvent(event: MigrationRequestEvent): Json = json"""{
    "categoryName": ${categoryName.value},
    "subscriber": {
      "version": ${event.subscriberVersion.value}
    }
  }"""

  implicit lazy val show: Show[MigrationRequestEvent] = Show.show { event =>
    show"subscriberVersion = ${event.subscriberVersion}"
  }
}
