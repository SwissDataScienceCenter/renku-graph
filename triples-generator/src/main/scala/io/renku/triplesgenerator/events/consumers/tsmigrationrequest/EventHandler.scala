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

package io.renku.triplesgenerator.events.consumers
package tsmigrationrequest

import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import com.typesafe.config.Config
import io.circe.Json
import io.renku.config.ServiceVersion
import io.renku.data.ErrorMessage
import io.renku.events.{consumers, CategoryName, EventRequestContent}
import io.renku.events.consumers.EventSchedulingResult.{SchedulingError, ServiceUnavailable}
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.Subscription.SubscriberUrl
import io.renku.events.consumers.{EventSchedulingResult, ProcessExecutor}
import io.renku.events.producers.EventSender
import io.renku.graph.config.EventLogUrl
import io.renku.metrics.MetricsRegistry
import io.renku.microservices.MicroserviceIdentifier
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.TSStateChecker.TSState
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    subscriberUrl:             SubscriberUrl,
    serviceId:                 MicroserviceIdentifier,
    serviceVersion:            ServiceVersion,
    tsStateChecker:            TSStateChecker[F],
    migrationsRunner:          MigrationsRunner[F],
    eventSender:               EventSender[F],
    subscriptionMechanism:     SubscriptionMechanism[F],
    processExecutor:           ProcessExecutor[F],
    override val categoryName: CategoryName = categoryName
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  protected override type Event = Unit

  import eventSender._
  import io.circe.literal._
  import io.renku.events.consumers.EventDecodingTools._
  import tsStateChecker._

  override def createHandlingDefinition(): EventHandlingProcess =
    EventHandlingProcess(
      decode,
      _ => runMigrations(),
      precondition = verifyTSState,
      onRelease = subscriptionMechanism.renewSubscription().some
    )

  private def decode: EventRequestContent => Either[Exception, Unit] = {
    case EventRequestContent.NoPayload(event: Json) => (decodeVersion andThenF checkVersionSupported)(event)
    case _                                          => new Exception("Invalid event").asLeft
  }

  private lazy val decodeVersion: Json => Either[Exception, ServiceVersion] =
    _.hcursor
      .downField("subscriber")
      .downField("version")
      .as[ServiceVersion]

  private lazy val checkVersionSupported: ServiceVersion => Either[Exception, Unit] = {
    case `serviceVersion` => ().asRight
    case version          => new Exception(show"Service in version '$serviceVersion' but event for '$version'").asLeft
  }

  private def runMigrations(): F[Unit] =
    migrationsRunner
      .run()
      .semiflatMap(_ => changeMigrationStatus("DONE"))
      .leftSemiflatMap(toRecoverableFailure)
      .merge
      .recoverWith(nonRecoverableFailure)

  private def changeMigrationStatus(status: String, maybeMessage: Option[String] = None): F[Unit] = sendEvent(
    EventRequestContent.NoPayload(json"""{
        "categoryName": "MIGRATION_STATUS_CHANGE",
        "subscriber": {
          "url":     ${subscriberUrl.value},
          "id":      ${serviceId.value},
          "version": ${serviceVersion.value}
        },
        "newStatus": $status
      }
      """.addIfDefined("message" -> maybeMessage)),
    EventSender.EventContext(CategoryName("MIGRATION_STATUS_CHANGE"),
                             show"$categoryName: sending status change event failed"
    )
  )

  private def toRecoverableFailure(recoverableFailure: ProcessingRecoverableError) = changeMigrationStatus(
    "RECOVERABLE_FAILURE",
    ErrorMessage.withMessageAndStackTrace(recoverableFailure.message, recoverableFailure.cause).value.some
  )

  private def nonRecoverableFailure: PartialFunction[Throwable, F[Unit]] = { case NonFatal(e) =>
    changeMigrationStatus("NON_RECOVERABLE_FAILURE", ErrorMessage.withStackTrace(e).value.some)
  }

  private def verifyTSState: F[Option[EventSchedulingResult]] =
    checkTSState
      .map {
        case TSState.Ready | TSState.MissingDatasets => None
        case TSState.ReProvisioning                  => ServiceUnavailable("Re-provisioning running").widen.some
      }
      .recover { case NonFatal(exception) => SchedulingError(exception).widen.some }
}

private object EventHandler {
  import eu.timepit.refined.auto._

  def apply[F[_]: Async: ReProvisioningStatus: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      subscriberUrl:         SubscriberUrl,
      serviceId:             MicroserviceIdentifier,
      serviceVersion:        ServiceVersion,
      subscriptionMechanism: SubscriptionMechanism[F],
      config:                Config
  ): F[consumers.EventHandler[F]] = for {
    tsStateChecker   <- TSStateChecker[F]
    migrationsRunner <- MigrationsRunner[F](config)
    eventSender      <- EventSender[F](EventLogUrl)
    processExecutor  <- ProcessExecutor.concurrent(processesCount = 1)
  } yield new EventHandler[F](subscriberUrl,
                              serviceId,
                              serviceVersion,
                              tsStateChecker,
                              migrationsRunner,
                              eventSender,
                              subscriptionMechanism,
                              processExecutor
  )
}
