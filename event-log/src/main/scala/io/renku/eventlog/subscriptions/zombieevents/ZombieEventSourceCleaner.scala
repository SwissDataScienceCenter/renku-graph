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

import cats.Parallel
import cats.data.Kleisli
import cats.effect.{Async, Bracket, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import eu.timepit.refined.api.Refined
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.subscriptions.SubscriptionTypeSerializers
import io.renku.eventlog.{EventLogDB, Microservice, TypeSerializers}
import skunk._
import skunk.implicits._

import scala.concurrent.ExecutionContext

private trait ZombieEventSourceCleaner[Interpretation[_]] {
  def removeZombieSources(): Interpretation[Unit]
}

private class ZombieEventSourceCleanerImpl[Interpretation[_]: Async: Parallel: Bracket[*[_], Throwable]: ContextShift](
    transactor:           SessionResource[Interpretation, EventLogDB],
    queriesExecTimes:     LabeledHistogram[Interpretation, SqlQuery.Name],
    microserviceBaseUrl:  MicroserviceBaseUrl,
    serviceHealthChecker: ServiceHealthChecker[Interpretation]
) extends DbClient(Some(queriesExecTimes))
    with ZombieEventSourceCleaner[Interpretation]
    with TypeSerializers
    with SubscriptionTypeSerializers {

  import serviceHealthChecker._

  override def removeZombieSources(): Interpretation[Unit] = transactor.use { implicit session =>
    session.transaction.use { transaction =>
      for {
        sp <- transaction.savepoint
        result <- findAndRemoveZombieSources recoverWith { error =>
                    transaction.rollback(sp).flatMap(_ => error.raiseError[Interpretation, Unit])
                  }
      } yield result
    }
  }

  private def findAndRemoveZombieSources(implicit session: Session[Interpretation]) = for {
    maybeZombieRecords <- findPotentialZombieRecords
    nonHealthySources  <- (maybeZombieRecords map isHealthy).parSequence.map(_.collect(nonHealthy))
    _                  <- (nonHealthySources map delete).sequence
  } yield ()

  private def findPotentialZombieRecords(implicit
      session: Session[Interpretation]
  ): Interpretation[List[(MicroserviceBaseUrl, SubscriberUrl)]] =
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Query[MicroserviceBaseUrl, (MicroserviceBaseUrl, SubscriberUrl)] = sql"""
            SELECT DISTINCT source_url, delivery_url
            FROM subscriber
            WHERE source_url <> $microserviceBaseUrlPut
            """.query(microserviceBaseUrlGet ~ subscriberUrlGet)
          session.prepare(query).use(_.stream(microserviceBaseUrl, 32).compile.toList)
        },
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find zombie sources")
      )
    }

  private def isHealthy
      : ((MicroserviceBaseUrl, SubscriberUrl)) => Interpretation[(MicroserviceBaseUrl, SubscriberUrl, Boolean)] = {
    case (sourceUrl, subscriberUrl) =>
      for {
        subscriberAsBaseUrl <- subscriberUrl.as[Interpretation, MicroserviceBaseUrl]
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

  private def delete(implicit
      session: Session[Interpretation]
  ): ((MicroserviceBaseUrl, SubscriberUrl)) => Interpretation[Unit] = { case (sourceUrl, subscriberUrl) =>
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Command[MicroserviceBaseUrl ~ SubscriberUrl] =
            sql"""DELETE
                  FROM subscriber
                  WHERE source_url = $microserviceBaseUrlPut AND delivery_url = $subscriberUrlPut
                  """.command
          session.prepare(query).use(_.execute(sourceUrl ~ subscriberUrl)).void
        },
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - delete zombie source")
      )
    }
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
