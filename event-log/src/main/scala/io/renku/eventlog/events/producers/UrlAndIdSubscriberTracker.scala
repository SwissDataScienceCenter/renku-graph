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

package io.renku.eventlog.events.producers

import cats.effect.MonadCancelThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.{Microservice, TypeSerializers}
import io.renku.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import skunk._
import skunk.data.Completion
import skunk.implicits._

private trait UrlAndIdSubscriberTracker[F[_]] extends SubscriberTracker[F, UrlAndIdSubscriptionInfo] {
  def add(subscriptionInfo: UrlAndIdSubscriptionInfo): F[Boolean]
  def remove(subscriberUrl: SubscriberUrl):            F[Boolean]
}

private class UrlAndIdSubscriberTrackerImpl[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes](
    sourceUrl: MicroserviceBaseUrl
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with UrlAndIdSubscriberTracker[F]
    with TypeSerializers {

  override def add(subscriptionInfo: UrlAndIdSubscriptionInfo): F[Boolean] = SessionResource[F].useK {
    measureExecutionTime(
      SqlStatement(name = "subscriber - add")
        .command[SubscriberId ~ SubscriberUrl ~ MicroserviceBaseUrl ~ SubscriberId](
          sql"""INSERT INTO subscriber (delivery_id, delivery_url, source_url)
                VALUES ($subscriberIdEncoder, $subscriberUrlEncoder, $microserviceBaseUrlEncoder)
                ON CONFLICT (delivery_url, source_url)
                DO UPDATE SET delivery_id = $subscriberIdEncoder, delivery_url = EXCLUDED.delivery_url, source_url = EXCLUDED.source_url
               """.command
        )
        .arguments(
          subscriptionInfo.subscriberId ~ subscriptionInfo.subscriberUrl ~ sourceUrl ~ subscriptionInfo.subscriberId
        )
        .build
    ) map insertToTableResult
  }

  override def remove(subscriberUrl: SubscriberUrl): F[Boolean] = SessionResource[F].useK {
    measureExecutionTime(
      SqlStatement(name = "subscriber - delete")
        .command[SubscriberUrl ~ MicroserviceBaseUrl](
          sql"""DELETE FROM subscriber
                WHERE delivery_url = $subscriberUrlEncoder AND source_url = $microserviceBaseUrlEncoder
            """.command
        )
        .arguments(subscriberUrl ~ sourceUrl)
        .build
    ) map deleteToTableResult
  }

  private lazy val deleteToTableResult: Completion => Boolean = {
    case Completion.Delete(0 | 1) => true
    case _                        => false
  }

  private lazy val insertToTableResult: Completion => Boolean = {
    case Completion.Insert(0 | 1) => true
    case _                        => false
  }
}

private object UrlAndIdSubscriberTracker {

  def apply[F[_]](implicit tracker: UrlAndIdSubscriberTracker[F]): UrlAndIdSubscriberTracker[F] = tracker

  def create[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]: F[UrlAndIdSubscriberTracker[F]] = for {
    microserviceUrlFinder <- MicroserviceUrlFinder(Microservice.ServicePort)
    sourceUrl             <- microserviceUrlFinder.findBaseUrl()
  } yield new UrlAndIdSubscriberTrackerImpl[F](sourceUrl)
}
