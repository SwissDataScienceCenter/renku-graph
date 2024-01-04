/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.entities.viewings.collector.projects
package activated

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.triplesgenerator.api.events.ProjectActivated
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait EventPersister[F[_]] {
  def persist(event: ProjectActivated): F[Unit]
}

private object EventPersister {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[EventPersister[F]] =
    ProjectsConnectionConfig
      .fromConfig[F]()
      .map(TSClient[F](_))
      .map(tsClient => new EventPersisterImpl[F](tsClient, EventDeduplicator(tsClient, categoryName)))

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): F[EventPersister[F]] = {
    val tsClient = TSClient[F](connectionConfig)
    val persister: EventPersister[F] = new EventPersisterImpl[F](tsClient, EventDeduplicator(tsClient, categoryName))
    persister.pure[F]
  }
}

private class EventPersisterImpl[F[_]: MonadThrow](tsClient: TSClient[F], deduplicator: EventDeduplicator[F])
    extends EventPersister[F] {

  import Encoder._
  import deduplicator.deduplicate
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._
  import io.renku.graph.model.{GraphClass, projects}
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore.client.syntax._
  import tsClient.{queryExpecting, upload}

  override def persist(event: ProjectActivated): F[Unit] =
    findProjectId(event) >>= {
      case None => ().pure[F]
      case Some(projectId) =>
        eventExists(projectId) >>= {
          case false => insert(projectId, event) >> deduplicate(projectId)
          case true  => ().pure[F]
        }
    }

  private def findProjectId(event: ProjectActivated) = queryExpecting {
    SparqlQuery.ofUnsafe(
      show"${categoryName.show.toLowerCase}: find id",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|SELECT DISTINCT ?id
               |WHERE {
               |  GRAPH ?id {
               |    ?id a schema:Project;
               |        renku:projectPath ${event.slug.asObject}
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

  private def eventExists(projectId: projects.ResourceId) = queryExpecting {
    SparqlQuery.ofUnsafe(
      show"${categoryName.show.toLowerCase}: check exists",
      Prefixes of renku -> "renku",
      sparql"""|SELECT ?date
               |WHERE {
               |  GRAPH ${GraphClass.ProjectViewedTimes.id} {
               |    ${projectId.asEntityId} renku:dateViewed ?date
               |  }
               |}
               |LIMIT 1
               |""".stripMargin
    )
  }(dateDecoder)
    .map(_.isDefined)

  private lazy val dateDecoder: Decoder[Option[projects.DateViewed]] = ResultsDecoder[Option, projects.DateViewed] {
    Decoder.instance[projects.DateViewed] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      extract[projects.DateViewed]("date")
    }
  }

  private def insert(projectId: projects.ResourceId, event: ProjectActivated): F[Unit] =
    upload(
      encode(ProjectViewing(projectId, projects.DateViewed(event.dateActivated.value)))
    )
}
