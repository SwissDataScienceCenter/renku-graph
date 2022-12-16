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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.Show
import cats.data.{EitherT, OptionT}
import cats.effect._
import cats.kernel.Monoid
import cats.syntax.all._
import fs2.Stream
import io.circe.Json
import io.circe.syntax._
import io.renku.compression.Zip
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.events.producers.EventSender
import io.renku.graph.config.EventLogUrl
import io.renku.graph.eventlog.EventLogClient
import io.renku.graph.eventlog.EventLogClient.{EventPayload, SearchCriteria}
import io.renku.graph.model.events.{EventInfo, EventStatus}
import io.renku.graph.model.projects.{Path => ProjectPath}
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.Migration
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.CompositePlanProvision.{Context, Result}
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.{AllProjects, MigrationExecutionRegister, RecoverableErrorsRecovery, RegisteredMigration}
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private[migrations] class CompositePlanProvision[F[_]: Sync: Logger](
    override val name: Migration.Name,
    allProjects:       AllProjects[F],
    eventSender:       EventSender[F],
    eventLogClient:    EventLogClient[F],
    executionRegister: MigrationExecutionRegister[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](name, executionRegister, recoveryStrategy) {

  protected override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] =
    EitherT(
      (Stream.eval(Logger[F].info("Starting composite plan migration")).drain ++ allProjects
        .findAll(50))
        .ifEmpty(Stream.eval(Logger[F].info("No projects found")).drain)
        .map(_.path)
        .evalMap(findLatestEvent)
        .unNone
        .evalMap(Context.create(getEventPayload))
        .evalMap {
          case Some(ctx) => sendStatusChange(ctx).as(Result.sendEvent)
          case None      => Result.skip.pure[F]
        }
        .compile
        .foldMonoid
        .flatTap(result => Logger[F].info(result.show))
        .void
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(recoveryStrategy.maybeRecoverableError[F, Unit])
    )

  def sendStatusChange(ctx: Context): F[Unit] = {
    val categoryName = CategoryName("EVENTS_STATUS_CHANGE")
    val eventData    = CompositePlanProvision.createStatusChangeEvent(ctx)
    Logger[F].info(show"Send status change to ${EventStatus.TriplesGenerated} for project ${ctx.event.project}") *>
      eventSender.sendEvent(
        EventRequestContent.NoPayload(eventData),
        EventSender.EventContext(categoryName, "Migration loading CompositePlans")
      )
  }

  def findLatestEvent(path: ProjectPath): F[Option[EventInfo]] =
    eventLogClient
      .getEvents(
        SearchCriteria
          .forProject(path)
          .withPerPage(1)
          .sortBy(SearchCriteria.Sort.EventDateDesc)
      )
      .map(_.toEither)
      .rethrow
      .map(_.headOption)
      .flatTap(optEvent => Logger[F].info(show"Project $path has latest event $optEvent"))

  def getEventPayload(event: EventInfo): F[Option[EventPayload]] =
    eventLogClient
      .getEventPayload(event.eventId, event.project.path)
      .map(_.toEither)
      .rethrow
}

object CompositePlanProvision {
  def create[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry]: F[CompositePlanProvision[F]] =
    for {
      allProjects <- AllProjects.create[F]
      eventSender <- EventSender[F]
      eventLogUrl <- EventLogUrl()
      elClient = EventLogClient(eventLogUrl)
      executionRegister <- MigrationExecutionRegister[F]
      name = Migration.Name("CompositePlanProvision")
    } yield new CompositePlanProvision[F](name, allProjects, eventSender, elClient, executionRegister)

  final case class Context(event: EventInfo, payload: Option[EventPayload], uncompressed: String)

  object Context {
    private[this] val eligibleEventStatus: Set[EventStatus] =
      EventStatus.all.diff(
        Set(
          EventStatus.New,
          EventStatus.Skipped,
          EventStatus.GeneratingTriples,
          EventStatus.GenerationRecoverableFailure,
          EventStatus.TriplesGenerated,
          EventStatus.TransformingTriples,
          EventStatus.TransformationRecoverableFailure,
          EventStatus.AwaitingDeletion,
          EventStatus.Deleting
        )
      )

    def create[F[_]: Logger: Sync](
        getEventPayload: EventInfo => F[Option[EventPayload]]
    )(ev:                EventInfo): F[Option[Context]] =
      OptionT
        .pure(ev)
        .filter(event => eligibleEventStatus.contains(event.status))
        .semiflatMap { event =>
          for {
            payload <- getEventPayload(event)
            plain <- payload match {
                       case Some(data) => Zip.unzip(data.data.toArray).attempt
                       case None       => Right("").pure[F]
                     }
            text <- plain.fold(
                      ex =>
                        Logger[F]
                          .warn(ex)(s"Unable to unzip event payload for ${event.eventId}")
                          .as("CompositePlan"), // send the event in this case
                      _.pure[F]
                    )
          } yield Context(event, payload, text)
        }
        .filter(ctx => ctx.payload.isEmpty || ctx.uncompressed.contains("CompositePlan"))
        .value

    implicit val show: Show[Context] =
      Show.show(ctx => s"${ctx.event.show}/payload:${ctx.payload.isDefined}")
  }

  final case class Result(skippedCount: Int, sendEvent: Int) {
    def total: Int = skippedCount + sendEvent
    def +(other: Result): Result = Result(skippedCount + other.skippedCount, sendEvent + other.sendEvent)
  }
  object Result {
    val zero: Result = Result(0, 0)

    def skip:      Result = Result(1, 0)
    def sendEvent: Result = Result(0, 1)

    implicit val monoid: Monoid[Result] =
      Monoid.instance(zero, _ + _)

    implicit val show: Show[Result] =
      Show.show(r =>
        s"Processed ${r.total} projects, ${r.skippedCount} skipped and ${r.sendEvent} changed status to ${EventStatus.TriplesGenerated.value}"
      )
  }

  def createStatusChangeEvent(ctx: Context): Json =
    Json.obj(
      "categoryName" -> "EVENTS_STATUS_CHANGE".asJson,
      "project" -> Json.obj(
        "id"   -> ctx.event.project.id.asJson,
        "path" -> ctx.event.project.path.asJson
      ),
      "newStatus" -> EventStatus.TriplesGenerated.asJson
    )
}
