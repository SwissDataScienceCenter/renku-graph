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

package io.renku.triplesgenerator.events.consumers
package tsmigrationrequest
package migrations
package v10migration

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.projects
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger
import tooling._

private class MigrationToV10[F[_]: Async: Logger](
    projectsFinder:    PagedProjectsFinder[F],
    eventsSender:      EventSender[F],
    executionRegister: MigrationExecutionRegister[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](MigrationToV10.name, executionRegister, recoveryStrategy) {

  import eventsSender._
  import fs2._
  import projectsFinder._
  import recoveryStrategy._

  protected[v10migration] override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    Stream
      .iterate(1)(_ + 1)
      .evalMap(_ => nextProjectsPage())
      .takeThrough(_.nonEmpty)
      .flatMap(in => Stream.emits(in))
      .map(toCleanUpEvent)
      .evalMap { case (payload, ctx) => sendEvent(payload, ctx) }
      .compile
      .drain
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  private def toCleanUpEvent(path: projects.Path): (EventRequestContent.NoPayload, EventSender.EventContext) = {
    val eventCategoryName = CategoryName("CLEAN_UP_REQUEST")
    EventRequestContent.NoPayload {
      json"""{
          "categoryName": $eventCategoryName,
          "project": {
            "path": $path
          }
        }"""
    } -> EventSender.EventContext(eventCategoryName, show"$categoryName: $name cannot send event for $path")
  }
}

private[migrations] object MigrationToV10 {
  val name: Migration.Name = Migration.Name("Migration to V10")

  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder]: F[Migration[F]] = for {
    projectsFinder    <- PagedProjectsFinder[F]
    eventsSender      <- EventSender[F](EventLogUrl)
    executionRegister <- MigrationExecutionRegister[F]
  } yield new MigrationToV10(projectsFinder, eventsSender, executionRegister)
}
