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
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import doobie.ConnectionIO
import eu.timepit.refined.api.Refined
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.{EventLogDB, Microservice, TypeSerializers}

import scala.concurrent.ExecutionContext

private trait ZombieEventSourceCleaner[Interpretation[_]] {
  def removeZombieSources(): Interpretation[Unit]
}

private class ZombieEventSourceCleanerImpl(transactor:           DbTransactor[IO, EventLogDB],
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
    actions            <- (maybeZombieRecords map toAction).parSequence.map(_.filter(_.actionable))
    _                  <- (actions map toQuery map execute).sequence
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
  } transact transactor.get

  private def toAction: ((MicroserviceBaseUrl, SubscriberUrl)) => IO[Action] = { case (sourceUrl, subscriberUrl) =>
    for {
      subscriberAsBaseUrl <- subscriberUrl.as[IO, MicroserviceBaseUrl]
      subscriberHealthy   <- ping(subscriberAsBaseUrl)
      sourceHealthy       <- ping(sourceUrl)
    } yield sourceHealthy -> subscriberHealthy match {
      case (true, _)      => NoAction
      case (false, false) => Delete(sourceUrl, subscriberUrl)
      case (false, true)  => Upsert(sourceUrl, subscriberUrl)
    }
  }

  private def toQuery: Action => ConnectionIO[Int] = {
    case Delete(sourceUrl, subscriberUrl, _) =>
      delete(sourceUrl, subscriberUrl)
    case Upsert(sourceUrl, subscriberUrl, _) =>
      checkIfExist(microserviceBaseUrl, subscriberUrl) flatMap {
        case true  => delete(sourceUrl, subscriberUrl)
        case false => move(sourceUrl, subscriberUrl)
      }
    case _ => 1.pure[ConnectionIO]
  }

  private def checkIfExist(sourceUrl: MicroserviceBaseUrl, subscriberUrl: SubscriberUrl) =
    measureExecutionTime {
      SqlQuery(
        sql"""|SELECT source_url
              |FROM subscriber
              |WHERE source_url = $sourceUrl AND delivery_url = $subscriberUrl
              |""".stripMargin.query[String].option.map(_.isDefined),
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - check source & delivery exists")
      )
    }

  private def delete(sourceUrl: MicroserviceBaseUrl, subscriberUrl: SubscriberUrl) =
    measureExecutionTime {
      SqlQuery(
        sql"""|DELETE
              |FROM subscriber
              |WHERE source_url = $sourceUrl AND delivery_url = $subscriberUrl
              |""".stripMargin.update.run,
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - delete zombie source")
      )
    }

  private def move(sourceUrl: MicroserviceBaseUrl, subscriberUrl: SubscriberUrl) =
    measureExecutionTime {
      SqlQuery(
        sql"""|UPDATE subscriber
              |SET source_url = $microserviceBaseUrl
              |WHERE source_url = $sourceUrl AND delivery_url = $subscriberUrl
              |""".stripMargin.update.run,
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - move subscriber")
      )
    }

  private def execute(query: ConnectionIO[Int]): IO[Unit] = query.transact(transactor.get).void

  private sealed trait Action { val actionable: Boolean }
  private case class Delete(source: MicroserviceBaseUrl, delivery: SubscriberUrl, actionable: Boolean = true)
      extends Action
  private case class Upsert(source: MicroserviceBaseUrl, delivery: SubscriberUrl, actionable: Boolean = true)
      extends Action
  private case object NoAction extends Action {
    val actionable: Boolean = false
  }
}

private object ZombieEventSourceCleaner {
  def apply(
      transactor:       DbTransactor[IO, EventLogDB],
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
