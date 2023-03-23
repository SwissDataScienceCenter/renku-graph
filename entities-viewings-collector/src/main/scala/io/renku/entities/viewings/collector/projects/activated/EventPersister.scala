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

package io.renku.entities.viewings.collector.projects
package activated

import cats.effect.Async
import cats.syntax.all._
import cats.MonadThrow
import io.renku.triplesgenerator.api.events.ProjectActivated
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait EventPersister[F[_]] {
  def persist(event: ProjectActivated): F[Unit]
}

private object EventPersister {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[EventPersister[F]] =
    ProjectsConnectionConfig[F]().map(TSClient[F](_)).map(new EventPersisterImpl[F](_))
}

private class EventPersisterImpl[F[_]: MonadThrow](tsClient: TSClient[F]) extends EventPersister[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.{projects, GraphClass}
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import ProjectViewingEncoder._
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.client.syntax._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import tsClient.{queryExpecting, upload}

  override def persist(event: ProjectActivated): F[Unit] =
    findProjectId(event) >>= {
      case None => ().pure[F]
      case Some(projectId) =>
        eventExists(projectId) >>= {
          case false => insert(projectId, event)
          case true  => ().pure[F]
        }
    }

  private def findProjectId(event: ProjectActivated) = queryExpecting {
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

  private def eventExists(projectId: projects.ResourceId) = queryExpecting {
    SparqlQuery.ofUnsafe(
      show"${categoryName.show.toLowerCase}: check exists",
      Prefixes of renku -> "renku",
      s"""|SELECT ?date
          |WHERE {
          |  GRAPH ${GraphClass.ProjectViewedTimes.id.sparql} {
          |    ${projectId.asEntityId.sparql} renku:dateViewed ?date
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