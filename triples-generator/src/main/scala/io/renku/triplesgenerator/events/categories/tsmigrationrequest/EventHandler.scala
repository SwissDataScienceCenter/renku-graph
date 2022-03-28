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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest

import cats.data.EitherT
import cats.data.EitherT.{left, leftT, rightT}
import cats.effect.kernel.Deferred
import cats.effect.{Async, Concurrent}
import cats.syntax.all._
import io.circe.Json
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, EventSchedulingResult}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.http.server.version.ServiceVersion
import org.typelevel.log4cats.Logger

private[events] class EventHandler[F[_]: Async: Logger](
    serviceVersion:             ServiceVersion,
    subscriptionMechanism:      SubscriptionMechanism[F],
    concurrentProcessesLimiter: ConcurrentProcessesLimiter[F],
    override val categoryName:  CategoryName = categoryName
) extends consumers.EventHandlerWithProcessLimiter[F](concurrentProcessesLimiter) {

  protected override def createHandlingProcess(request: EventRequestContent): F[EventHandlingProcess[F]] =
    EventHandlingProcess.withWaitingForCompletion[F](
      deferred => startEventProcessing(request, deferred),
      subscriptionMechanism.renewSubscription()
    )

  private def startEventProcessing(request: EventRequestContent, deferred: Deferred[F, Unit]) = for {
    event            <- toJson(request)
    requestedVersion <- decodeVersion(event)
    _                <- checkVersionSupported(requestedVersion)
    result <- Concurrent[F]
//                .start(eventProcessor.process(commitEvent) >> deferred.complete(()))
                .start(deferred.complete(()))
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(requestedVersion))
                .leftSemiflatTap(Logger[F].log(requestedVersion))
  } yield result

  private def toJson: EventRequestContent => EitherT[F, EventSchedulingResult, Json] = {
    case EventRequestContent.NoPayload(event: Json) => rightT(event)
    case _                                          => leftT[F, Json](BadRequest).leftWiden[EventSchedulingResult]
  }

  private def decodeVersion(event: Json): EitherT[F, EventSchedulingResult, ServiceVersion] = EitherT.fromEither[F] {
    event.hcursor
      .downField("subscriber")
      .downField("version")
      .as[ServiceVersion]
      .leftMap(_ => BadRequest)
  }

  private def checkVersionSupported: ServiceVersion => EitherT[F, EventSchedulingResult, Unit] = {
    case `serviceVersion` => rightT(())
    case version =>
      left {
        Logger[F]
          .logInfo(version, show"$BadRequest service in version '$serviceVersion'")
          .map(_ => BadRequest)
      }
  }
}

private[events] object EventHandler {
  import eu.timepit.refined.auto._

  def apply[F[_]: Async: Logger](
      subscriptionMechanism: SubscriptionMechanism[F]
  ): F[EventHandler[F]] = for {
    serviceVersion           <- ServiceVersion.readFromConfig[F]()
    concurrentProcessLimiter <- ConcurrentProcessesLimiter(1)
  } yield new EventHandler[F](serviceVersion, subscriptionMechanism, concurrentProcessLimiter)
}
