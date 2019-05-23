/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import ch.datascience.graph.acceptancetests.stubs.RdfStoreStub
import ch.datascience.graph.acceptancetests.tooling.WebhookServiceClient.WebhookServiceClient
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.triplesgenerator
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.ExecutionContext

trait GraphServices extends BeforeAndAfterAll with ExternalServiceStubbing {
  this: Suite =>

  import ch.datascience.{tokenrepository, webhookservice}
  import eu.timepit.refined.auto._

  protected override val maybeFixedPort: Option[Int Refined Positive] = Some(2048)

  protected implicit lazy val executionContext: ExecutionContext = GraphServices.executionContext
  protected implicit lazy val contextShift:     ContextShift[IO] = GraphServices.contextShift
  protected implicit lazy val timer:            Timer[IO]        = GraphServices.timer

  protected val webhookServiceClient:  WebhookServiceClient = GraphServices.webhookServiceClient
  protected val tokenRepositoryClient: ServiceClient        = GraphServices.tokenRepositoryClient

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    GraphServices.servicesRunner
      .run(
        ServiceRun(webhookservice.Microservice, webhookServiceClient),
        ServiceRun(tokenrepository.Microservice, tokenRepositoryClient),
        ServiceRun(
          service       = triplesgenerator.Microservice,
          serviceClient = GraphServices.triplesGeneratorClient,
          preServiceStart = List(
            IO(RdfStoreStub.start()),
            IO(RdfStoreStub.givenRenkuDataSetExists())
          ),
          postServiceStart = List(
            IO(RdfStoreStub.shutdown()),
            IO(RDFStore.start())
          )
        )
      )
      .unsafeRunSync()
  }
}

object GraphServices {

  implicit lazy val executionContext: ExecutionContext = ExecutionContext.global
  implicit lazy val contextShift:     ContextShift[IO] = IO.contextShift(executionContext)
  implicit lazy val timer:            Timer[IO]        = IO.timer(executionContext)

  val webhookServiceClient   = WebhookServiceClient()
  val triplesGeneratorClient = TriplesGeneratorClient()
  val tokenRepositoryClient  = TokenRepositoryClient()

  private val servicesRunner = (Semaphore[IO](1) map (new ServicesRunner(_))).unsafeRunSync()
}
