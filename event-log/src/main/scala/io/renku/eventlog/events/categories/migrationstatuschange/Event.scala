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

package io.renku.eventlog.events.categories.migrationstatuschange

import io.renku.eventlog.MigrationStatus.{Done, NonRecoverableFailure, RecoverableFailure}
import io.renku.eventlog.{MigrationMessage, MigrationStatus}
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.http.server.version.ServiceVersion

private trait Event extends Product with Serializable {
  type NewStatus <: MigrationStatus
  val subscriberUrl:     SubscriberUrl
  val subscriberVersion: ServiceVersion
  val newStatus:         NewStatus
}

private object Event {
  import cats.Show
  import cats.syntax.all._

  implicit val show: Show[Event] = Show.show {
    case ToDone(url, version)                  => show"url = $url, version = $version, status = $Done"
    case ToRecoverableFailure(url, version, _) => show"url = $url, version = $version, status = $RecoverableFailure"
    case ToNonRecoverableFailure(url, version, _) =>
      show"url = $url, version = $version, status = $NonRecoverableFailure"
  }

  final case class ToDone(subscriberUrl: SubscriberUrl, subscriberVersion: ServiceVersion) extends Event {
    type NewStatus = Done
    val newStatus: NewStatus = Done
  }

  final case class ToNonRecoverableFailure(subscriberUrl:     SubscriberUrl,
                                           subscriberVersion: ServiceVersion,
                                           message:           MigrationMessage
  ) extends Event {
    type NewStatus = NonRecoverableFailure
    val newStatus: NewStatus = NonRecoverableFailure
  }

  final case class ToRecoverableFailure(subscriberUrl:     SubscriberUrl,
                                        subscriberVersion: ServiceVersion,
                                        message:           MigrationMessage
  ) extends Event {
    type NewStatus = RecoverableFailure
    val newStatus: NewStatus = RecoverableFailure
  }
}
