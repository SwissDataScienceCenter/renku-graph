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

import cats.effect.{BracketThrow, IO}
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
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

private class SubscriberTrackerImpl[Interpretation[_]: BracketThrow](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    sourceUrl:        MicroserviceBaseUrl
) extends DbClient(Some(queriesExecTimes))
    with SubscriberTracker[Interpretation]
    with TypeSerializers {

  override def add(subscriptionInfo: SubscriptionInfo): Interpretation[Boolean] = sessionResource.useK {
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

  override def remove(subscriberUrl: SubscriberUrl): Interpretation[Boolean] = sessionResource.useK {
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

private object SubscriberTracker {
  def apply(sessionResource:  SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name]
  ): IO[SubscriberTracker[IO]] = for {
    microserviceUrlFinder <- MicroserviceUrlFinder(Microservice.ServicePort)
    sourceUrl             <- microserviceUrlFinder.findBaseUrl()
  } yield new SubscriberTrackerImpl(sessionResource, queriesExecTimes, sourceUrl)
}
