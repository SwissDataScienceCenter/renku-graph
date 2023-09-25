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

package io.renku.eventlog.events.producers

import cats.effect.Async
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.{Microservice, TypeSerializers}
import io.renku.events.DefaultSubscription.DefaultSubscriber
import io.renku.events.Subscription.{SubscriberId, SubscriberUrl}
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import skunk._
import skunk.data.Completion
import skunk.implicits._

private trait DefaultSubscriberTracker[F[_]] extends SubscriberTracker[F, DefaultSubscriber] {
  def add(subscriber:       DefaultSubscriber): F[Boolean]
  def remove(subscriberUrl: SubscriberUrl):     F[Boolean]
}

private object DefaultSubscriberTracker {

  def apply[F[_]](implicit tracker: DefaultSubscriberTracker[F]): DefaultSubscriberTracker[F] = tracker

  def create[F[_]: Async: SessionResource: QueriesExecutionTimes]: F[DefaultSubscriberTracker[F]] = for {
    microserviceUrlFinder <- MicroserviceUrlFinder(Microservice.ServicePort)
    sourceUrl             <- microserviceUrlFinder.findBaseUrl()
  } yield new DefaultSubscriberTrackerImpl[F](sourceUrl)
}

private class DefaultSubscriberTrackerImpl[F[_]: Async: SessionResource: QueriesExecutionTimes](
    sourceUrl: MicroserviceBaseUrl
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with DefaultSubscriberTracker[F]
    with TypeSerializers {

  override def add(subscriber: DefaultSubscriber): F[Boolean] = SessionResource[F].useK {
    measureExecutionTime(
      SqlStatement
        .named("subscriber - add")
        .command[SubscriberId *: SubscriberUrl *: MicroserviceBaseUrl *: SubscriberId *: EmptyTuple](
          sql"""INSERT INTO subscriber (delivery_id, delivery_url, source_url)
                VALUES ($subscriberIdEncoder, $subscriberUrlEncoder, $microserviceBaseUrlEncoder)
                ON CONFLICT (delivery_url, source_url)
                DO UPDATE SET delivery_id = $subscriberIdEncoder, delivery_url = EXCLUDED.delivery_url, source_url = EXCLUDED.source_url
               """.command
        )
        .arguments(subscriber.id *: subscriber.url *: sourceUrl *: subscriber.id *: EmptyTuple)
        .build
    ) map insertToTableResult
  }

  override def remove(subscriberUrl: SubscriberUrl): F[Boolean] = SessionResource[F].useK {
    measureExecutionTime(
      SqlStatement
        .named("subscriber - delete")
        .command[SubscriberUrl *: MicroserviceBaseUrl *: EmptyTuple](
          sql"""DELETE FROM subscriber
                WHERE delivery_url = $subscriberUrlEncoder AND source_url = $microserviceBaseUrlEncoder
            """.command
        )
        .arguments(subscriberUrl *: sourceUrl *: EmptyTuple)
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
