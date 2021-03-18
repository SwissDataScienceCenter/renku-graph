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

package io.renku.eventlog.subscriptions.zombieevents

import cats.effect.{Bracket, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import eu.timepit.refined.api.Refined
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.{EventLogDB, Microservice, TypeSerializers}

import scala.concurrent.ExecutionContext

private trait ZombieEventSourceCleaner[Interpretation[_]] {
  def removeZombieSources(): Interpretation[Unit]
}

private class ZombieEventSourceCleanerImpl(transactor:           SessionResource[IO, EventLogDB],
                                           queriesExecTimes:     LabeledHistogram[IO, SqlQuery.Name],
                                           microserviceBaseUrl:  MicroserviceBaseUrl,
                                           serviceHealthChecker: ServiceHealthChecker[IO]
)(implicit ME:                                                   Bracket[IO, Throwable], contextShift: ContextShift[IO])
    extends DbClient(Some(queriesExecTimes))
    with ZombieEventSourceCleaner[IO]
    with TypeSerializers {

  import doobie.implicits._
  import serviceHealthChecker._

  override def removeZombieSources(): IO[Unit] = for {
    maybeZombieRecords <- findPotentialZombieRecords
    nonHealthySources  <- (maybeZombieRecords map isHealthy).parSequence.map(_.collect(nonHealthy))
    _                  <- (nonHealthySources map delete).sequence
  } yield ()

  private def findPotentialZombieRecords: IO[List[(MicroserviceBaseUrl, SubscriberUrl)]] = measureExecutionTime {
    SqlQuery(
      sql"""|SELECT DISTINCT source_url, delivery_url
            |FROM subscriber
            |WHERE source_url <> $microserviceBaseUrl
            |""".stripMargin
        .query[(MicroserviceBaseUrl, SubscriberUrl)]
        .to[List],
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find zombie sources")
    )
  } transact transactor.resource

  private def isHealthy: ((MicroserviceBaseUrl, SubscriberUrl)) => IO[(MicroserviceBaseUrl, SubscriberUrl, Boolean)] = {
    case (sourceUrl, subscriberUrl) =>
      for {
        subscriberAsBaseUrl <- subscriberUrl.as[IO, MicroserviceBaseUrl]
        subscriberHealthy   <- ping(subscriberAsBaseUrl)
        sourceHealthy       <- ping(sourceUrl)
      } yield sourceHealthy -> subscriberHealthy match {
        case (true, _)  => (sourceUrl, subscriberUrl, true)
        case (false, _) => (sourceUrl, subscriberUrl, subscriberHealthy)
      }
  }

  private lazy val nonHealthy
      : PartialFunction[(MicroserviceBaseUrl, SubscriberUrl, Boolean), (MicroserviceBaseUrl, SubscriberUrl)] = {
    case (serviceUrl, subscriberUrl, false) => serviceUrl -> subscriberUrl
  }

  private def delete: ((MicroserviceBaseUrl, SubscriberUrl)) => IO[Unit] = { case (sourceUrl, subscriberUrl) =>
    measureExecutionTime {
      SqlQuery(
        sql"""|DELETE
              |FROM subscriber
              |WHERE source_url = $sourceUrl AND delivery_url = $subscriberUrl
              |""".stripMargin.update.run,
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - delete zombie source")
      )
    }.transact(transactor.resource).void
  }
}

private object ZombieEventSourceCleaner {
  def apply(
      transactor:       SessionResource[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
      logger:           Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ZombieEventSourceCleaner[IO]] = for {
    serviceUrlFinder     <- MicroserviceUrlFinder(Microservice.ServicePort)
    serviceBaseUrl       <- serviceUrlFinder.findBaseUrl()
    serviceHealthChecker <- ServiceHealthChecker(logger)
  } yield new ZombieEventSourceCleanerImpl(transactor, queriesExecTimes, serviceBaseUrl, serviceHealthChecker)
}
