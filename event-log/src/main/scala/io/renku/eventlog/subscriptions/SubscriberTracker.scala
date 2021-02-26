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

import cats.effect.{Bracket, IO}
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.{EventLogDB, Microservice, TypeSerializers}

private trait SubscriberTracker[Interpretation[_]] {
  def add(subscriberUrl:    SubscriberUrl): Interpretation[Boolean]
  def remove(subscriberUrl: SubscriberUrl): Interpretation[Boolean]
}

private class SubscriberTrackerImpl(transactor:       DbTransactor[IO, EventLogDB],
                                    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
                                    sourceUrl:        MicroserviceBaseUrl
)(implicit ME:                                        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with SubscriberTracker[IO]
    with TypeSerializers {

  override def add(subscriberUrl: SubscriberUrl): IO[Boolean] = measureExecutionTime(
    SqlQuery(
      sql"""|INSERT INTO subscriber (delivery_url, source_url)
            |VALUES ($subscriberUrl, $sourceUrl)
            |ON CONFLICT (delivery_url, source_url)
            |DO NOTHING
            |""".stripMargin.update.run,
      name = "subscriber - add"
    )
  ) transact transactor.get map mapToTableResult

  override def remove(subscriberUrl: SubscriberUrl): IO[Boolean] = measureExecutionTime(
    SqlQuery(
      sql"""|DELETE FROM subscriber
            |WHERE delivery_url = $subscriberUrl AND source_url = $sourceUrl
            |""".stripMargin.update.run,
      name = "subscriber - delete"
    )
  ) transact transactor.get map mapToTableResult

  private lazy val mapToTableResult: Int => Boolean = {
    case 0 | 1 => true
    case _     => false
  }
}

private object SubscriberTracker {
  def apply(transactor:       DbTransactor[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  ): IO[SubscriberTracker[IO]] = for {
    microserviceUrlFinder <- MicroserviceUrlFinder(Microservice.ServicePort)
    sourceUrl             <- microserviceUrlFinder.findBaseUrl()
  } yield new SubscriberTrackerImpl(transactor, queriesExecTimes, sourceUrl)
}
