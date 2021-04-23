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
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.db.implicits._
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import eu.timepit.refined.api.Refined
import io.renku.eventlog.{EventLogDB, Microservice, TypeSerializers}
import org.typelevel.log4cats.Logger
import skunk._
import skunk.data.Completion
import skunk.implicits._

import scala.concurrent.ExecutionContext

private trait ZombieNodesCleaner[Interpretation[_]] {
  def removeZombieNodes(): Interpretation[Unit]
}

private class ZombieNodesCleanerImpl[Interpretation[_]: Async: Parallel: Bracket[*[_], Throwable]: ContextShift](
    sessionResource:      SessionResource[Interpretation, EventLogDB],
    queriesExecTimes:     LabeledHistogram[Interpretation, SqlStatement.Name],
    microserviceBaseUrl:  MicroserviceBaseUrl,
    serviceHealthChecker: ServiceHealthChecker[Interpretation]
) extends DbClient(Some(queriesExecTimes))
    with ZombieNodesCleaner[Interpretation]
    with TypeSerializers {

  import serviceHealthChecker._

  override def removeZombieNodes(): Interpretation[Unit] = sessionResource.useK {
    for {
      maybeZombieRecords <- findPotentialZombieRecords
      actions            <- Kleisli.liftF((maybeZombieRecords map toAction).parSequence.map(_.filter(_.actionable)))
      _                  <- (actions map toQuery map execute).sequence
    } yield ()
  }

  private lazy val findPotentialZombieRecords = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find zombie sources"))
      .select[Void, (MicroserviceBaseUrl, SubscriberUrl)](
        sql"""SELECT DISTINCT source_url, delivery_url
                FROM subscriber
          """
          .query(microserviceBaseUrlDecoder ~ subscriberUrlDecoder)
          .map { case sourceUrl ~ subscriberUrl => (sourceUrl, subscriberUrl) }
      )
      .arguments(Void)
      .build(_.toList)
  }

  private lazy val toAction: ((MicroserviceBaseUrl, SubscriberUrl)) => Interpretation[Action] = {
    case (sourceUrl, subscriberUrl) =>
      for {
        subscriberAsBaseUrl <- subscriberUrl.as[Interpretation, MicroserviceBaseUrl]
        subscriberHealthy   <- ping(subscriberAsBaseUrl)
        sourceHealthy       <- ping(sourceUrl)
      } yield sourceHealthy -> subscriberHealthy match {
        case (true, true)  => NoAction
        case (false, true) => Upsert(sourceUrl, subscriberUrl)
        case (_, false)    => Delete(sourceUrl, subscriberUrl)
      }
  }

  private lazy val toQuery: Action => Kleisli[Interpretation, Session[Interpretation], Completion] = {
    case Delete(sourceUrl, subscriberUrl, _) =>
      delete(sourceUrl, subscriberUrl)
    case Upsert(sourceUrl, subscriberUrl, _) =>
      checkIfExist(microserviceBaseUrl, subscriberUrl) flatMap {
        case true  => delete(sourceUrl, subscriberUrl)
        case false => move(sourceUrl, subscriberUrl)
      }
    case _ => Kleisli.pure(Completion.Delete(1)).widen[Completion]
  }

  private def checkIfExist(sourceUrl: MicroserviceBaseUrl, subscriberUrl: SubscriberUrl) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - check source & delivery exists"))
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
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - delete zombie source"))
      .command[MicroserviceBaseUrl ~ SubscriberUrl](sql"""
          DELETE
          FROM subscriber
          WHERE source_url = $microserviceBaseUrlEncoder AND delivery_url = $subscriberUrlEncoder
          """.command)
      .arguments(sourceUrl ~ subscriberUrl)
      .build
  }

  private def move(sourceUrl: MicroserviceBaseUrl, subscriberUrl: SubscriberUrl) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - move subscriber"))
      .command[MicroserviceBaseUrl ~ MicroserviceBaseUrl ~ SubscriberUrl](sql"""
         UPDATE subscriber
         SET source_url = $microserviceBaseUrlEncoder
         WHERE source_url = $microserviceBaseUrlEncoder AND delivery_url = $subscriberUrlEncoder
        """.command)
      .arguments(microserviceBaseUrl ~ sourceUrl ~ subscriberUrl)
      .build
  }

  private def execute(
      query: Kleisli[Interpretation, Session[Interpretation], Completion]
  ): Kleisli[Interpretation, Session[Interpretation], Unit] = query.void

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
  def apply(
      sessionResource:  SessionResource[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name],
      logger:           Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ZombieNodesCleaner[IO]] = for {
    serviceUrlFinder     <- MicroserviceUrlFinder(Microservice.ServicePort)
    serviceBaseUrl       <- serviceUrlFinder.findBaseUrl()
    serviceHealthChecker <- ServiceHealthChecker(logger)
  } yield new ZombieNodesCleanerImpl(sessionResource, queriesExecTimes, serviceBaseUrl, serviceHealthChecker)
}
