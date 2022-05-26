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

package io.renku.eventlog.subscriptions.zombieevents

import cats.Parallel
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import io.renku.db.implicits._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.{Microservice, TypeSerializers}
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.http.client.ServiceHealthChecker
import io.renku.metrics.LabeledHistogram
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import org.typelevel.log4cats.Logger
import skunk._
import skunk.data.Completion
import skunk.implicits._

private trait ZombieNodesCleaner[F[_]] {
  def removeZombieNodes(): F[Unit]
}

private class ZombieNodesCleanerImpl[F[_]: Async: Parallel: SessionResource](
    queriesExecTimes:     LabeledHistogram[F],
    microserviceBaseUrl:  MicroserviceBaseUrl,
    serviceHealthChecker: ServiceHealthChecker[F]
) extends DbClient(Some(queriesExecTimes))
    with ZombieNodesCleaner[F]
    with TypeSerializers {

  override def removeZombieNodes(): F[Unit] = SessionResource[F].useK {
    for {
      maybeZombieRecords <- findPotentialZombieRecords
      actions            <- Kleisli.liftF((maybeZombieRecords map toAction).parSequence.map(_.filter(_.actionable)))
      _                  <- (actions map toQuery map execute).sequence
    } yield ()
  }

  private lazy val findPotentialZombieRecords = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - find zombie sources")
      .select[Void, (MicroserviceBaseUrl, SubscriberUrl)](
        sql"""SELECT DISTINCT source_url, delivery_url FROM subscriber"""
          .query(microserviceBaseUrlDecoder ~ subscriberUrlDecoder)
          .map { case sourceUrl ~ subscriberUrl => (sourceUrl, subscriberUrl) }
      )
      .arguments(Void)
      .build(_.toList)
  }

  private lazy val toAction: ((MicroserviceBaseUrl, SubscriberUrl)) => F[Action] = { case (sourceUrl, subscriberUrl) =>
    for {
      subscriberAsBaseUrl <- subscriberUrl.as[F, MicroserviceBaseUrl]
      subscriberHealthy   <- ping(subscriberAsBaseUrl, ifNot = microserviceBaseUrl)
      sourceHealthy       <- ping(sourceUrl, ifNot = microserviceBaseUrl)
    } yield sourceHealthy -> subscriberHealthy match {
      case (true, true)  => NoAction
      case (false, true) => Upsert(sourceUrl, subscriberUrl)
      case (_, false)    => Delete(sourceUrl, subscriberUrl)
    }
  }

  private def ping(url: MicroserviceBaseUrl, ifNot: MicroserviceBaseUrl) =
    if (url == ifNot) true.pure[F]
    else serviceHealthChecker.ping(url)

  private lazy val toQuery: Action => Kleisli[F, Session[F], Completion] = {
    case Delete(sourceUrl, subscriberUrl, _) => delete(sourceUrl, subscriberUrl)
    case Upsert(sourceUrl, subscriberUrl, _) =>
      checkIfExist(microserviceBaseUrl, subscriberUrl) >>= {
        case true  => delete(sourceUrl, subscriberUrl)
        case false => move(sourceUrl, subscriberUrl)
      }
    case _ => Kleisli.pure(Completion.Delete(1)).widen[Completion]
  }

  private def checkIfExist(sourceUrl: MicroserviceBaseUrl, subscriberUrl: SubscriberUrl) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - check source & delivery exists")
      .select[MicroserviceBaseUrl ~ SubscriberUrl, MicroserviceBaseUrl](
        sql"""
            SELECT source_url
            FROM subscriber
            WHERE source_url = $microserviceBaseUrlEncoder AND delivery_url = $subscriberUrlEncoder
          """.query(microserviceBaseUrlDecoder)
      )
      .arguments(sourceUrl ~ subscriberUrl)
      .build(_.option)
      .mapResult(_.isDefined)
  }

  private def delete(sourceUrl: MicroserviceBaseUrl, subscriberUrl: SubscriberUrl) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - delete zombie source")
      .command[MicroserviceBaseUrl ~ SubscriberUrl](sql"""
          DELETE
          FROM subscriber
          WHERE source_url = $microserviceBaseUrlEncoder AND delivery_url = $subscriberUrlEncoder
          """.command)
      .arguments(sourceUrl ~ subscriberUrl)
      .build
  }

  private def move(sourceUrl: MicroserviceBaseUrl, subscriberUrl: SubscriberUrl) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - move subscriber")
      .command[MicroserviceBaseUrl ~ MicroserviceBaseUrl ~ SubscriberUrl](sql"""
         UPDATE subscriber
         SET source_url = $microserviceBaseUrlEncoder
         WHERE source_url = $microserviceBaseUrlEncoder AND delivery_url = $subscriberUrlEncoder
        """.command)
      .arguments(microserviceBaseUrl ~ sourceUrl ~ subscriberUrl)
      .build
  }

  private def execute(query: Kleisli[F, Session[F], Completion]): Kleisli[F, Session[F], Unit] = query.void

  private sealed trait Action {
    val actionable: Boolean
  }

  private case class Delete(source: MicroserviceBaseUrl, delivery: SubscriberUrl, actionable: Boolean = true)
      extends Action

  private case class Upsert(source: MicroserviceBaseUrl, delivery: SubscriberUrl, actionable: Boolean = true)
      extends Action

  private case object NoAction extends Action {
    val actionable: Boolean = false
  }
}

private object ZombieNodesCleaner {
  def apply[F[_]: Async: Parallel: SessionResource: Logger](
      queriesExecTimes: LabeledHistogram[F]
  ): F[ZombieNodesCleaner[F]] = for {
    serviceUrlFinder     <- MicroserviceUrlFinder(Microservice.ServicePort)
    serviceBaseUrl       <- serviceUrlFinder.findBaseUrl()
    serviceHealthChecker <- ServiceHealthChecker[F]
  } yield new ZombieNodesCleanerImpl(queriesExecTimes, serviceBaseUrl, serviceHealthChecker)
}
