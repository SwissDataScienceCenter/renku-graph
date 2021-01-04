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

package ch.datascience.graph.acceptancetests.tooling

import cats.effect._
import cats.effect.concurrent.Semaphore
import ch.datascience.db.DBConfigProvider
import ch.datascience.graph.acceptancetests.db.{EventLog, TokenRepository}
import ch.datascience.graph.acceptancetests.stubs.GitLab
import ch.datascience.graph.acceptancetests.tooling.KnowledgeGraphClient.KnowledgeGraphClient
import ch.datascience.graph.acceptancetests.tooling.WebhookServiceClient.WebhookServiceClient
import io.renku.eventlog
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.ExecutionContext

trait GraphServices extends BeforeAndAfterAll {
  this: Suite =>

  protected implicit lazy val executionContext: ExecutionContext = GraphServices.executionContext
  protected implicit lazy val contextShift:     ContextShift[IO] = GraphServices.contextShift
  protected implicit lazy val timer:            Timer[IO]        = GraphServices.timer

  protected val restClient:             RestClient           = new RestClient()
  protected val webhookServiceClient:   WebhookServiceClient = GraphServices.webhookServiceClient
  protected val tokenRepositoryClient:  ServiceClient        = GraphServices.tokenRepositoryClient
  protected val triplesGeneratorClient: ServiceClient        = GraphServices.triplesGeneratorClient
  protected val knowledgeGraphClient:   KnowledgeGraphClient = GraphServices.knowledgeGraphClient
  protected val eventLogClient:         ServiceClient        = GraphServices.eventLogClient
  protected val webhookService:         ServiceRun           = GraphServices.webhookService
  protected val tokenRepository:        ServiceRun           = GraphServices.tokenRepository
  protected val triplesGenerator:       ServiceRun           = GraphServices.triplesGenerator
  protected val knowledgeGraph:         ServiceRun           = GraphServices.knowledgeGraph
  protected val eventLog:               ServiceRun           = GraphServices.eventLog

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    GraphServices.servicesRunner
      .run(
        tokenRepository,
        eventLog,
        webhookService,
        triplesGenerator,
        knowledgeGraph
      )
      .unsafeRunSync()
  }
}

object GraphServices {

  import ch.datascience._
  import ch.datascience.graph.acceptancetests.stubs.RdfStoreStub

  implicit lazy val executionContext: ExecutionContext = ExecutionContext.global
  implicit lazy val contextShift:     ContextShift[IO] = IO.contextShift(executionContext)
  implicit lazy val timer:            Timer[IO]        = IO.timer(executionContext)

  val webhookServiceClient   = WebhookServiceClient()
  val triplesGeneratorClient = TriplesGeneratorClient()
  val tokenRepositoryClient  = TokenRepositoryClient()
  val knowledgeGraphClient   = KnowledgeGraphClient()
  val eventLogClient         = EventLogClient()

  val webhookService = ServiceRun("webhook-service", webhookservice.Microservice, webhookServiceClient)
  val knowledgeGraph = ServiceRun("knowledge-graph", knowledgegraph.Microservice, knowledgeGraphClient)
  val tokenRepository = ServiceRun(
    "token-repository",
    tokenrepository.Microservice,
    tokenRepositoryClient,
    preServiceStart = List(TokenRepository.startDB()),
    serviceArgsList = List(() => s"${DBConfigProvider.JdbcUrl}=${TokenRepository.jdbcUrl}")
  )
  val eventLog = ServiceRun(
    "event-log",
    eventlog.Microservice,
    eventLogClient,
    preServiceStart = List(EventLog.startDB()),
    serviceArgsList = List(() => s"${DBConfigProvider.JdbcUrl}=${EventLog.jdbcUrl}")
  )
  val triplesGenerator = ServiceRun(
    "triples-generator",
    service = triplesgenerator.Microservice,
    serviceClient = triplesGeneratorClient,
    preServiceStart = List(RDFStore.stop(), IO(RdfStoreStub.start()), IO(RdfStoreStub.givenRenkuDatasetExists())),
    postServiceStart = List(IO(RdfStoreStub.shutdown()), RDFStore.start())
  )

  private val servicesRunner = (Semaphore[IO](1) map (new ServicesRunner(_))).unsafeRunSync()

  def restart(service: ServiceRun): Unit = servicesRunner.restart(service)

  sys.addShutdownHook {
    servicesRunner.stopAllServices()
    GitLab.shutdown()
  }
}
