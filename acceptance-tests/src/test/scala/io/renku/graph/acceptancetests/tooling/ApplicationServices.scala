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

package io.renku.graph.acceptancetests.tooling

import cats.Monad
import cats.effect._
import cats.effect.std.Semaphore
import io.renku._
import io.renku.eventlog.MigrationStatus
import io.renku.graph.acceptancetests.db.{EventLog, TokenRepository, TriplesGenerator, TriplesStore}
import io.renku.graph.acceptancetests.stubs.RemoteTriplesGenerator
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabStubSupport
import io.renku.graph.acceptancetests.tooling.KnowledgeGraphClient.KnowledgeGraphClient
import io.renku.graph.acceptancetests.tooling.WebhookServiceClient.WebhookServiceClient
import io.renku.triplesstore.FusekiUrl
import org.scalatest.BeforeAndAfterAll
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait ApplicationServices extends GitLabStubSupport with RemoteTriplesGenerator with BeforeAndAfterAll {
  self: AcceptanceSpec =>

  protected implicit lazy val fusekiUrl: FusekiUrl = TriplesStore.fusekiUrl

  val restClient:               RestClientImpl                = new RestClientImpl()
  val webhookServiceClient:     WebhookServiceClient          = WebhookServiceClient()
  val commitEventServiceClient: ServiceClient                 = CommitEventServiceClient()
  val triplesGeneratorClient:   ServiceClient                 = TriplesGeneratorClient()
  val tokenRepositoryClient:    ServiceClient                 = TokenRepositoryClient()
  val knowledgeGraphClient:     KnowledgeGraphClient          = KnowledgeGraphClient()
  val eventLogClient:           EventLogClient.EventLogClient = EventLogClient()

  def run(service: ServiceRun): Unit = servicesRunner.run(service).unsafeRunSync()

  def stop(service: ServiceRun): Unit = servicesRunner.stop(service)

  override def beforeAll(): Unit = {
    super.beforeAll()

    servicesRunner
      .run(
        tokenRepository,
        eventLog,
        webhookService,
        commitEventService,
        triplesGenerator,
        knowledgeGraph
      )
      .unsafeRunSync()
  }

  private lazy val webhookService = ServiceRun("webhook-service", webhookservice.Microservice, webhookServiceClient)
  private lazy val commitEventService = ServiceRun(
    "commit-event-service",
    commiteventservice.Microservice,
    commitEventServiceClient
  )
  private lazy val knowledgeGraph = ServiceRun("knowledge-graph", knowledgegraph.Microservice, knowledgeGraphClient)
  private lazy val tokenRepository = ServiceRun(
    "token-repository",
    tokenrepository.Microservice,
    tokenRepositoryClient,
    preServiceStart = List(TokenRepository.startDB()),
    serviceArgsList = List()
  )
  private lazy val eventLog = ServiceRun(
    "event-log",
    eventlog.Microservice,
    eventLogClient,
    preServiceStart = List(EventLog.startDB()),
    postServiceStart = List(eventLogClient.waitForReadiness),
    serviceArgsList = List()
  )
  protected lazy val triplesGenerator: ServiceRun = ServiceRun(
    "triples-generator",
    service = triplesgenerator.Microservice,
    serviceClient = triplesGeneratorClient,
    preServiceStart = List(TriplesGenerator.startDB(), TriplesStore.start()),
    postServiceStart = List(waitForTSMigrations)
  )

  private def waitForTSMigrations: IO[Unit] =
    Monad[IO].whileM_ {
      EventLog.findTSMigrationsStatus.flatMap {
        case Some(status @ MigrationStatus.Done) => Logger[IO].info(s"TS migrations status: $status").as(false)
        case other => Logger[IO].info(s"TS migrations status: ${other getOrElse "unknown"}; waiting").as(true)
      }
    }(Temporal[IO] sleep (1 second))

  private lazy val servicesRunner = (Semaphore[IO](1) map (new ServicesRunner(_))).unsafeRunSync()
}
