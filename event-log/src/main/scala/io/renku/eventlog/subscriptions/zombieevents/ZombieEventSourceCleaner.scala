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
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
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
    otherSources      <- findPotentialZombieSources
    nonHealthySources <- (otherSources map isHealthy).parSequence.map(_.collect(nonHealthy))
    _                 <- (nonHealthySources map delete).sequence
  } yield ()

  private def findPotentialZombieSources: IO[List[MicroserviceBaseUrl]] = measureExecutionTime {
    SqlQuery(
      sql"""|SELECT DISTINCT source_url
            |FROM subscriber
            |WHERE source_url <> $microserviceBaseUrl
            |""".stripMargin
        .query[MicroserviceBaseUrl]
        .to[List],
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find zombie sources")
    )
  } transact transactor.get

  private def isHealthy(serviceUrl: MicroserviceBaseUrl): IO[(MicroserviceBaseUrl, Boolean)] =
    ping(serviceUrl) map (serviceUrl -> _)

  private lazy val nonHealthy: PartialFunction[(MicroserviceBaseUrl, Boolean), MicroserviceBaseUrl] = {
    case (serviceUrl, false) => serviceUrl
  }

  private def delete(serviceUrl: MicroserviceBaseUrl): IO[Unit] = {
    measureExecutionTime {
      SqlQuery(
        sql"""|DELETE
              |FROM subscriber
              |WHERE source_url = $serviceUrl
              |""".stripMargin.update.run,
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - delete zombie source")
      )
    } transact transactor.get
  }.void
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
