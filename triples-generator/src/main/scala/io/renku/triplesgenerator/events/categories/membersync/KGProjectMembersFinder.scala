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
import io.renku.graph.config.RenkuBaseUrlLoader
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.projects.{Path, ResourceId}
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{RenkuBaseUrl, persons, projects}
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait KGProjectMembersFinder[F[_]] {
  def findProjectMembers(path: projects.Path): F[Set[KGProjectMember]]
}

private class KGProjectMembersFinderImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    renkuBaseUrl:   RenkuBaseUrl,
    timeRecorder:   SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with KGProjectMembersFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder

  def findProjectMembers(path: projects.Path): F[Set[KGProjectMember]] =
    queryExpecting[Set[KGProjectMember]](using = query(path))

  private implicit lazy val recordsDecoder: Decoder[Set[KGProjectMember]] = { cursor =>
    import Decoder._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val member: Decoder[KGProjectMember] = { cursor =>
      for {
        memberId <- cursor.downField("memberId").downField("value").as[persons.ResourceId]
        gitLabId <- cursor.downField("gitLabId").downField("value").as[GitLabId]
      } yield KGProjectMember(memberId, gitLabId)
    }

    cursor.downField("results").downField("bindings").as(decodeList(member)).map(_.toSet)
  }

  private def query(path: Path) = SparqlQuery.of(
    name = "members by project path",
    Prefixes of schema -> "schema",
    s"""|SELECT DISTINCT ?memberId ?gitLabId
        |WHERE {
        |  ${ResourceId(path)(renkuBaseUrl).showAs[RdfResource]} a schema:Project;
        |                                                        schema:member ?memberId.                                                     
        |  ?sameAsId  schema:additionalType  'GitLab';
        |             schema:identifier      ?gitLabId ;
        |             ^schema:sameAs         ?memberId .
        |}
        |""".stripMargin
  )
}

private object KGProjectMembersFinder {
  def apply[F[_]: Async: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[KGProjectMembersFinder[F]] = for {
    rdfStoreConfig <- RdfStoreConfig[F]()
    renkuBaseUrl   <- RenkuBaseUrlLoader[F]()
  } yield new KGProjectMembersFinderImpl(rdfStoreConfig, renkuBaseUrl, timeRecorder)
}

private final case class KGProjectMember(resourceId: persons.ResourceId, gitLabId: persons.GitLabId)
