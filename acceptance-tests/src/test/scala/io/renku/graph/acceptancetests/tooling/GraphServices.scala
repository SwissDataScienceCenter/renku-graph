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

package io.renku.graph.acceptancetests.tooling

import cats.effect._
import cats.effect.std.Semaphore
import io.renku._
import io.renku.graph.acceptancetests.db.{EventLog, TokenRepository}
import io.renku.graph.acceptancetests.stubs.{GitLab, RemoteTriplesGenerator}
import io.renku.graph.acceptancetests.tooling.KnowledgeGraphClient.KnowledgeGraphClient
import io.renku.graph.acceptancetests.tooling.WebhookServiceClient.WebhookServiceClient
import io.renku.graph.config.RenkuBaseUrlLoader
import io.renku.graph.model.RenkuBaseUrl
import io.renku.rdfstore.FusekiBaseUrl
import io.renku.testtools.IOSpec
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.typelevel.log4cats.Logger

import scala.util.Try

trait GraphServices extends GitLab with RemoteTriplesGenerator with IOSpec with BeforeAndAfterAll {
  self: Suite =>

  protected implicit val fusekiBaseUrl: FusekiBaseUrl = RDFStore.fusekiBaseUrl
  implicit val renkuBaseUrl:            RenkuBaseUrl  = RenkuBaseUrlLoader[Try]().fold(throw _, identity)
  implicit lazy val logger:             Logger[IO]    = TestLogger()

  val restClient:               RestClientImpl                = new RestClientImpl()
  val webhookServiceClient:     WebhookServiceClient          = WebhookServiceClient()
  val commitEventServiceClient: ServiceClient                 = CommitEventServiceClient()
  val triplesGeneratorClient:   ServiceClient                 = TriplesGeneratorClient()
  val tokenRepositoryClient:    ServiceClient                 = TokenRepositoryClient()
  val knowledgeGraphClient:     KnowledgeGraphClient          = KnowledgeGraphClient()
  val eventLogClient:           EventLogClient.EventLogClient = EventLogClient()

  def run(service: ServiceRun): Unit = servicesRunner.run(service).unsafeRunSync()

  def stop(service: ServiceRun): Unit = servicesRunner.stop(service)

  protected override def beforeAll(): Unit = {
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
    preServiceStart = List(RDFStore.stop(), RDFStore.start())
  )

  private lazy val servicesRunner = (Semaphore[IO](1) map (new ServicesRunner(_))).unsafeRunSync()
}
