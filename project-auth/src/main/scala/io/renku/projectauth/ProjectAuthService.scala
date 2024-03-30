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

package io.renku.projectauth

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.effect._
import cats.syntax.all._
import fs2.io.net.Network
import fs2.{Pipe, Stream}
import io.renku.graph.model.projects.Slug
import io.renku.graph.model.{RenkuUrl, Schemas}
import io.renku.jsonld.NamedGraph
import io.renku.jsonld.syntax._
import io.renku.projectauth.util.ProjectAuthDataRow
import io.renku.triplesstore.client.http.{ConnectionConfig, SparqlClient}
import io.renku.triplesstore.client.sparql.Fragment
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

/** Manage authorization data for projects and members. */
trait ProjectAuthService[F[_]] {

  def update(data: ProjectAuthData): F[Unit]

  def updateAll: Pipe[F, ProjectAuthData, Nothing]

  def remove(slugs: NonEmptyList[Slug]): F[Unit]
  def remove(slug:  Slug, more: Slug*): F[Unit] = remove(NonEmptyList(slug, more.toList))

  def getAll(filter: QueryFilter, chunkSize: Int = 100): Stream[F, ProjectAuthData]
}

object ProjectAuthService {

  def resource[F[_]: Async: Network: Logger](
      connectionConfig: ConnectionConfig,
      timeout:          Duration = 20.minutes
  )(implicit renkuUrl: RenkuUrl): Resource[F, ProjectAuthService[F]] =
    SparqlClient(connectionConfig, timeout).map(c => apply[F](c, renkuUrl))

  def apply[F[_]: MonadThrow](client: SparqlClient[F], renkuUrl: RenkuUrl): ProjectAuthService[F] =
    new Impl[F](client, renkuUrl)

  private final class Impl[F[_]: MonadThrow](sparqlClient: SparqlClient[F], renkuUrl: RenkuUrl)
      extends ProjectAuthService[F] {
    private[this] val graph = ProjectAuth.graph
    private implicit val rUrl: RenkuUrl = renkuUrl

    override def remove(slugs: NonEmptyList[Slug]): F[Unit] =
      sparqlClient.update(sparql"""${"schema" -> Schemas.schema}
                                  |${"renku" -> Schemas.renku}
                                  |
                                  |DELETE { Graph $graph {?s ?p ?o} }
                                  |WHERE {
                                  |  Graph $graph {
                                  |    ?s a schema:Project;
                                  |       renku:slug ?slug;
                                  |       ?p ?o.
                                  |    VALUES(?slug) { ${slugs.toList.map(_.value)} }
                                  |  }
                                  |}
          """.stripMargin)

    override def update(data: ProjectAuthData): F[Unit] = {
      val jsonld = NamedGraph.fromJsonLDsUnsafe(graph, data.asJsonLD)
      remove(data.slug) >> sparqlClient.upload(jsonld)
    }

    override def updateAll: Pipe[F, ProjectAuthData, Nothing] =
      _.chunks
        .evalTap(_.toNel.map(_.map(_.slug)).map(remove).getOrElse(().pure[F]))
        .map(chunk =>
          chunk.toNel match { // TODO improve that ergonomics for NamedGraph in jsonld4s
            case Some(nel) => NamedGraph.fromJsonLDsUnsafe(graph, nel.head.asJsonLD, nel.tail.map(_.asJsonLD): _*)
            case None      => NamedGraph(graph, Seq.empty)
          }
        )
        .evalMap(sparqlClient.upload)
        .drain

    override def getAll(filter: QueryFilter, chunkSize: Int): Stream[F, ProjectAuthData] =
      streamAll(filter, chunkSize)

    private def streamAll(filter: QueryFilter, chunkSize: Int) =
      Stream
        .iterate(0)(_ + chunkSize)
        .evalMap(offset => getChunk(filter, chunkSize, offset))
        .takeWhile(_.nonEmpty)
        .flatMap(Stream.emits)
        .through(ProjectAuthDataRow.collect)

    private def getChunk(filter: QueryFilter, limit: Int, offset: Int): F[List[ProjectAuthDataRow]] =
      sparqlClient.queryDecode[ProjectAuthDataRow](
        sparql"""PREFIX schema: <http://schema.org/>
                |PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>
                |
                |SELECT ?slug ?visibility ?memberRole
                |WHERE {
                |  Graph ${graph.asSparql} {
                |    ?project a schema:Project;
                |             renku:slug ?slug;
                |             renku:visibility ?visibility.
                |    OPTIONAL {
                |      ?project renku:memberRole ?memberRole.
                |    }
                |    ${slugFilter(filter)}
                |    ${memberFilter(filter)}
                |  }
                |}
                |ORDER BY ?slug
                |OFFSET $offset
                |LIMIT $limit
                |""".stripMargin
      )

    private def slugFilter(f: QueryFilter) =
      f.slug.map(s => sparql"?project renku:slug ${s.value}.").getOrElse(Fragment.empty)

    private def memberFilter(f: QueryFilter) =
      f.member match {
        case s if s.isEmpty   => Fragment.empty
        case s if s.size == 1 => sparql"?project renku:memberId ${s.head.value}"
        case s =>
          sparql"""
                  |?project renku:memberId ?memberId.
                  |VALUES (?memberId) {
                  |  ${s.map(_.value)}
                  |}.
                  |""".stripMargin
      }
  }
}
