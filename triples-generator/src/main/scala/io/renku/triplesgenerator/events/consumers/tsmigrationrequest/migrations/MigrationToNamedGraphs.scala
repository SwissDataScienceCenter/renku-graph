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

package io.renku.triplesgenerator.events.consumers
package tsmigrationrequest
package migrations

import DefaultGraphProjectsFinder.ProjectInfo
import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.circe.literal._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.graph.model.events.EventStatus.TriplesGenerated
import io.renku.graph.model.{GraphClass, Schemas, projects}
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger
import tooling.{RecordsFinder, RecoverableErrorsRecovery}

private class MigrationToNamedGraphs[F[_]: Async](
    defaultGraphProjectsFinder: DefaultGraphProjectsFinder[F],
    namedGraphsProjectFinder:   NamedGraphsProjectFinder[F],
    eventsSender:               EventSender[F],
    recoveryStrategy:           RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends Migration[F] {

  import defaultGraphProjectsFinder._
  import eventsSender._
  import fs2._
  import namedGraphsProjectFinder._
  import recoveryStrategy._

  override lazy val name: Migration.Name = MigrationToNamedGraphs.name

  override def run(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    Stream
      .iterate(1)(_ + 1)
      .evalMap(findDefaultGraphProjects)
      .takeThrough(_.nonEmpty)
      .flatMap(in => Stream.emits(in))
      .evalFilterNot { case (projectId, _) => checkExists(projectId) }
      .map(toRedoEvent)
      .evalMap { case (payload, ctx) => sendEvent(payload, ctx) }
      .compile
      .drain
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  private lazy val toRedoEvent: ProjectInfo => (EventRequestContent.NoPayload, EventSender.EventContext) = {
    case (_, path) =>
      val eventCategoryName = CategoryName("EVENTS_STATUS_CHANGE")
      EventRequestContent.NoPayload {
        json"""{
          "categoryName": $eventCategoryName,
          "project": {
            "path": $path
          },
          "newStatus": $TriplesGenerated
        }"""
      } -> EventSender.EventContext(eventCategoryName, show"$categoryName: $name cannot send event for $path")
  }
}

private object MigrationToNamedGraphs {
  val name: Migration.Name = Migration.Name("Migration to Named Graphs")

  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder] = for {
    defaultGraphProjectsFinder <- DefaultGraphProjectsFinder[F]
    namedGraphsProjectFinder   <- NamedGraphsProjectFinder[F]
    eventsSender               <- EventSender[F]
  } yield new MigrationToNamedGraphs(defaultGraphProjectsFinder, namedGraphsProjectFinder, eventsSender)
}

private trait DefaultGraphProjectsFinder[F[_]] {
  def findDefaultGraphProjects(page: Int): F[List[ProjectInfo]]
}

private object DefaultGraphProjectsFinder {

  type ProjectInfo = (projects.ResourceId, projects.Path)

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[DefaultGraphProjectsFinder[F]] =
    RenkuConnectionConfig[F]().map(RecordsFinder[F](_)).map(new DefaultGraphProjectsFinderImpl(_))
}

private class DefaultGraphProjectsFinderImpl[F[_]](recordsFinder: RecordsFinder[F])
    extends DefaultGraphProjectsFinder[F]
    with Schemas {

  import ResultsDecoder._
  import io.renku.tinytypes.json.TinyTypeDecoders._

  private val chunkSize: Int = 30

  override def findDefaultGraphProjects(page: Int): F[List[ProjectInfo]] =
    recordsFinder.findRecords(query(page))

  private def query(page: Int) = SparqlQuery.of(
    MigrationToNamedGraphs.name.asRefined,
    Prefixes of (schema -> "schema", renku -> "renku"),
    s"""|SELECT DISTINCT ?id ?path
        |WHERE {
        |  ?id a schema:Project;
        |      renku:projectPath ?path
        |}
        |ORDER BY ?path
        |LIMIT $chunkSize
        |OFFSET ${(page - 1) * chunkSize}
        |""".stripMargin
  )

  private implicit lazy val decoder: Decoder[List[ProjectInfo]] = ResultsDecoder[List, ProjectInfo] { implicit cur =>
    (extract[projects.ResourceId]("id") -> extract[projects.Path]("path")).mapN(_ -> _)
  }
}

private trait NamedGraphsProjectFinder[F[_]] {
  def checkExists(resourceId: projects.ResourceId): F[Boolean]
}

private object NamedGraphsProjectFinder {
  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder]: F[NamedGraphsProjectFinder[F]] =
    ProjectsConnectionConfig[F]().map(RecordsFinder[F](_)).map(new NamedGraphsProjectFinderImpl[F](_))
}

private class NamedGraphsProjectFinderImpl[F[_]: MonadThrow](recordsFinder: RecordsFinder[F])
    extends NamedGraphsProjectFinder[F]
    with Schemas {

  import ResultsDecoder._
  import io.renku.graph.model.views.RdfResource
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def checkExists(resourceId: projects.ResourceId): F[Boolean] =
    recordsFinder.findRecords[projects.ResourceId](query(resourceId)) map {
      case Nil => false
      case _   => true
    }

  private def query(id: projects.ResourceId) = SparqlQuery.of(
    MigrationToNamedGraphs.name.asRefined,
    Prefixes of (schema -> "schema", renku -> "renku"),
    s"""|SELECT DISTINCT ?id
        |FROM <${GraphClass.Project.id(id)}> {
        |  BIND (${id.showAs[RdfResource]} AS ?id)
        |  ?id a schema:Project
        |}
        |""".stripMargin
  )

  private implicit lazy val decoder: Decoder[List[projects.ResourceId]] =
    ResultsDecoder[List, projects.ResourceId](implicit cur => extract[projects.ResourceId]("id"))
}
