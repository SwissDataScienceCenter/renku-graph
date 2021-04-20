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

import cats.data.Kleisli
import cats.effect.{Async, Bracket, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import eu.timepit.refined.auto._
import io.renku.eventlog.{EventLogDB, Microservice, TypeSerializers}
import skunk._
import skunk.data.Completion
import skunk.implicits._

private trait SubscriberTracker[Interpretation[_]] {
  def add(subscriptionInfo: SubscriptionInfo): Interpretation[Boolean]

  def remove(subscriberUrl: SubscriberUrl): Interpretation[Boolean]
}

private class SubscriberTrackerImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name],
    sourceUrl:        MicroserviceBaseUrl
) extends DbClient(Some(queriesExecTimes))
    with SubscriberTracker[Interpretation]
    with TypeSerializers {

  override def add(subscriptionInfo: SubscriptionInfo): Interpretation[Boolean] = sessionResource.useK {
    measureExecutionTimeK(
      SqlQuery(
        Kleisli { session =>
          val query: Command[SubscriberId ~ SubscriberUrl ~ MicroserviceBaseUrl ~ SubscriberId] =
            sql"""INSERT INTO subscriber (delivery_id, delivery_url, source_url)
                  VALUES ($subscriberIdPut, $subscriberUrlPut, $microserviceBaseUrlPut)
                  ON CONFLICT (delivery_url, source_url)
                  DO UPDATE SET delivery_id = $subscriberIdPut, delivery_url = EXCLUDED.delivery_url, source_url = EXCLUDED.source_url
               """.command
          session.prepare(query).use {
            _.execute(
              subscriptionInfo.subscriberId ~ subscriptionInfo.subscriberUrl ~ sourceUrl ~ subscriptionInfo.subscriberId
            )
          }
        },
        name = "subscriber - add"
      )
    ) map insertToTableResult
  }

  override def remove(subscriberUrl: SubscriberUrl): Interpretation[Boolean] = sessionResource.useK {
    measureExecutionTimeK(
      SqlQuery(
        Kleisli { session =>
          val query: Command[SubscriberUrl ~ MicroserviceBaseUrl] =
            sql"""
            DELETE FROM subscriber
            WHERE delivery_url = $subscriberUrlPut AND source_url = $microserviceBaseUrlPut
          """.command
          session.prepare(query).use(_.execute(subscriberUrl ~ sourceUrl))
        },
        name = "subscriber - delete"
      )
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

private object SubscriberTracker {
  def apply(sessionResource:  SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  ): IO[SubscriberTracker[IO]] = for {
    microserviceUrlFinder <- MicroserviceUrlFinder(Microservice.ServicePort)
    sourceUrl             <- microserviceUrlFinder.findBaseUrl()
  } yield new SubscriberTrackerImpl(sessionResource, queriesExecTimes, sourceUrl)
}
