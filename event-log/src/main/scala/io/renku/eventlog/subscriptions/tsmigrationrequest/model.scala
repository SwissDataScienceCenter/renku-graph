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

package io.renku.eventlog.subscriptions.tsmigrationrequest

import cats.Show
import cats.syntax.all._
import io.circe.Decoder
import io.renku.data.ErrorMessage
import io.renku.eventlog.subscriptions.{Capacity, SubscriptionInfo}
import io.renku.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import io.renku.http.server.version.ServiceVersion
import io.renku.tinytypes.constraints.{InstantNotInTheFuture, NonBlank}
import io.renku.tinytypes.{InstantTinyType, StringTinyType, TinyTypeFactory}

import java.time.Instant

private final case class MigratorSubscriptionInfo(subscriberUrl:     SubscriberUrl,
                                                  subscriberId:      SubscriberId,
                                                  subscriberVersion: ServiceVersion
) extends SubscriptionInfo {
  override val maybeCapacity: Option[Capacity] = None
}

private object MigratorSubscriptionInfo {
  implicit lazy val show: Show[MigratorSubscriptionInfo] = Show.show { info =>
    import info._
    show"subscriber = $subscriberUrl, id = $subscriberId, version = $subscriberVersion"
  }
}

private sealed trait MigrationStatus extends StringTinyType with Product with Serializable

private object MigrationStatus extends TinyTypeFactory[MigrationStatus](MigrationStatusInstantiator) {

  val all: Set[MigrationStatus] = Set(New, Sent, Done, NonRecoverableFailure, RecoverableFailure)

  type New = New.type
  final case object New extends MigrationStatus {
    override val value: String = "NEW"
  }

  type Sent = Sent.type
  final case object Sent extends MigrationStatus {
    override val value: String = "SENT"
  }

  type Done = Done.type
  final case object Done extends MigrationStatus {
    override val value: String = "DONE"
  }

  type NonRecoverableFailure = NonRecoverableFailure.type
  final case object NonRecoverableFailure extends MigrationStatus {
    override val value: String = "NON_RECOVERABLE_FAILURE"
  }

  type RecoverableFailure = RecoverableFailure.type
  final case object RecoverableFailure extends MigrationStatus {
    override val value: String = "RECOVERABLE_FAILURE"
  }

  import io.circe.Decoder.decodeString
  implicit val decoder: Decoder[MigrationStatus] = decodeString.emap { value =>
    Either.fromOption(
      MigrationStatus.all.find(_.value == value),
      ifNone = s"'$value' unknown MigrationStatus"
    )
  }
}

private object MigrationStatusInstantiator extends (String => MigrationStatus) {
  override def apply(value: String): MigrationStatus = MigrationStatus.all.find(_.value == value).getOrElse {
    throw new IllegalArgumentException(s"'$value' unknown MigrationStatus")
  }
}

final class ChangeDate private (val value: Instant) extends AnyVal with InstantTinyType
object ChangeDate extends TinyTypeFactory[ChangeDate](new ChangeDate(_)) with InstantNotInTheFuture {
  import io.renku.tinytypes.json.TinyTypeDecoders.instantDecoder
  implicit val decoder: Decoder[ChangeDate] = instantDecoder(ChangeDate)
}

final class MigrationMessage private (val value: String) extends AnyVal with StringTinyType
object MigrationMessage extends TinyTypeFactory[MigrationMessage](new MigrationMessage(_)) with NonBlank {

  def apply(exception: Throwable): MigrationMessage = MigrationMessage(ErrorMessage.withStackTrace(exception).value)

  import io.renku.tinytypes.json.TinyTypeDecoders.stringDecoder
  implicit val decoder: Decoder[MigrationMessage] = stringDecoder(MigrationMessage)
}
