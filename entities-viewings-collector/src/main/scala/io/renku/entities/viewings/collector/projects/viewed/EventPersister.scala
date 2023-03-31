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

package io.renku.entities.viewings.collector
package projects
package viewed

import cats.effect.Async
import cats.syntax.all._
import cats.MonadThrow
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger
import persons.{GLUserViewedProject, PersonViewedProjectPersister, Project}

private[viewings] trait EventPersister[F[_]] {
  def persist(event: ProjectViewedEvent): F[Unit]
}

private[viewings] object EventPersister {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[EventPersister[F]] =
    ProjectsConnectionConfig[F]()
      .map(TSClient[F](_))
      .map(tsClient => new EventPersisterImpl[F](tsClient, PersonViewedProjectPersister(tsClient)))
}

private[viewings] class EventPersisterImpl[F[_]: MonadThrow](
    tsClient:                     TSClient[F],
    personViewedProjectPersister: PersonViewedProjectPersister[F]
) extends EventPersister[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.{projects, GraphClass}
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.client.syntax._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import tsClient.{queryExpecting, updateWithNoResult, upload}
  import Encoder._

  override def persist(event: ProjectViewedEvent): F[Unit] =
    findProjectId(event) >>= {
      case None            => ().pure[F]
      case Some(projectId) => persistIfOlderOrNone(event, projectId) >> persistPersonViewedProject(event, projectId)
    }

  private def persistIfOlderOrNone(event: ProjectViewedEvent, projectId: projects.ResourceId) =
    findStoredDate(projectId) >>= {
      case None => insert(projectId, event)
      case Some(date) if date < event.dateViewed =>
        deleteOldViewedDate(projectId) >> insert(projectId, event)
      case _ => ().pure[F]
    }

  private def findProjectId(event: ProjectViewedEvent) = queryExpecting {
    SparqlQuery.ofUnsafe(
      show"${categoryName.show.toLowerCase}: find id",
      Prefixes of (renku -> "renku", schema -> "schema"),
      s"""|SELECT DISTINCT ?id
          |WHERE {
          |  GRAPH ?id {
          |    ?id a schema:Project;
          |        renku:projectPath ${event.path.asObject.asSparql.sparql}
          |  }
          |}
          |""".stripMargin
    )
  }(idDecoder)

  private lazy val idDecoder: Decoder[Option[projects.ResourceId]] = ResultsDecoder[Option, projects.ResourceId] {
    Decoder.instance[projects.ResourceId] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      extract[projects.ResourceId]("id")
    }
  }

  private def findStoredDate(id: projects.ResourceId): F[Option[projects.DateViewed]] =
    queryExpecting {
      SparqlQuery.ofUnsafe(
        show"${categoryName.show.toLowerCase}: find date",
        Prefixes of renku -> "renku",
        s"""|SELECT (MAX(?date) AS ?mostRecentDate)
            |WHERE {
            |  GRAPH ${GraphClass.ProjectViewedTimes.id.sparql} {
            |    BIND (${id.asEntityId.sparql} AS ?id)
            |    ?id renku:dateViewed ?date
            |  }
            |}
            |GROUP BY ?id
            |""".stripMargin
      )
    }(dateDecoder)

  private lazy val dateDecoder: Decoder[Option[projects.DateViewed]] = ResultsDecoder[Option, projects.DateViewed] {
    Decoder.instance[projects.DateViewed] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      extract[projects.DateViewed]("mostRecentDate")
    }
  }

  private def deleteOldViewedDate(projectId: projects.ResourceId): F[Unit] = updateWithNoResult(
    SparqlQuery.ofUnsafe(
      show"${categoryName.show.toLowerCase}: delete",
      Prefixes of renku -> "renku",
      s"""|DELETE { GRAPH ${GraphClass.ProjectViewedTimes.id.sparql} { ?id ?p ?o } }
          |WHERE {
          |  GRAPH ${GraphClass.ProjectViewedTimes.id.sparql} {
          |    BIND (${projectId.asEntityId.sparql} AS ?id)
          |    ?id ?p ?o 
          |  }
          |}
          |""".stripMargin
    )
  )

  private def insert(projectId: projects.ResourceId, event: ProjectViewedEvent): F[Unit] =
    upload(
      encode(ProjectViewing(projectId, event.dateViewed))
    )

  private def persistPersonViewedProject(event: ProjectViewedEvent, projectId: projects.ResourceId) =
    event.maybeUserId
      .map(GLUserViewedProject(_, Project(projectId, event.path), event.dateViewed))
      .map(personViewedProjectPersister.persist)
      .getOrElse(().pure[F])
}
