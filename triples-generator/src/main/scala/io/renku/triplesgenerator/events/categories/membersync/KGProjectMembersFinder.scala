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

package io.renku.triplesgenerator.events.categories.membersync

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.projects.{Path, ResourceId}
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{RenkuUrl, persons, projects}
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait KGProjectMembersFinder[F[_]] {
  def findProjectMembers(path: projects.Path): F[Set[KGProjectMember]]
}

private class KGProjectMembersFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    rdfStoreConfig: RdfStoreConfig,
    renkuUrl:       RenkuUrl
) extends RdfStoreClientImpl(rdfStoreConfig)
    with KGProjectMembersFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder

  def findProjectMembers(path: projects.Path): F[Set[KGProjectMember]] =
    queryExpecting[Set[KGProjectMember]](using = query(path))

  private implicit lazy val recordsDecoder: Decoder[Set[KGProjectMember]] = ResultsDecoder[Set, KGProjectMember] {
    implicit cursor =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      (extract[persons.ResourceId]("memberId") -> extract[persons.GitLabId]("gitLabId"))
        .mapN(KGProjectMember)
  }

  private def query(path: Path) = SparqlQuery.of(
    name = "members by project path",
    Prefixes of schema -> "schema",
    s"""|SELECT DISTINCT ?memberId ?gitLabId
        |WHERE {
        |  ${ResourceId(path)(renkuUrl).showAs[RdfResource]} a schema:Project;
        |                                                        schema:member ?memberId.                                                     
        |  ?sameAsId  schema:additionalType  'GitLab';
        |             schema:identifier      ?gitLabId ;
        |             ^schema:sameAs         ?memberId .
        |}
        |""".stripMargin
  )
}

private object KGProjectMembersFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[KGProjectMembersFinder[F]] = for {
    rdfStoreConfig <- RdfStoreConfig[F]()
    renkuUrl       <- RenkuUrlLoader[F]()
  } yield new KGProjectMembersFinderImpl(rdfStoreConfig, renkuUrl)
}

private final case class KGProjectMember(resourceId: persons.ResourceId, gitLabId: persons.GitLabId)
