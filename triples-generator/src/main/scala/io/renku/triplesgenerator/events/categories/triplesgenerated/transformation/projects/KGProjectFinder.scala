/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.projects

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.projects
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.projects.KGProjectFinder.KGProjectInfo
import org.typelevel.log4cats.Logger

private trait KGProjectFinder[F[_]] {
  def find(resourceId: projects.ResourceId): F[Option[KGProjectInfo]]
}

private class KGProjectFinderImpl[F[_]: Async: Logger](rdfStoreConfig: RdfStoreConfig,
                                                       timeRecorder: SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with KGProjectFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import Decoder._
  import io.renku.graph.model.Schemas._
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def find(resourceId: projects.ResourceId): F[Option[KGProjectInfo]] =
    queryExpecting[List[KGProjectInfo]](using = query(resourceId)) >>= toSingleResult(resourceId)

  private def query(resourceId: projects.ResourceId) = SparqlQuery.of(
    name = "transformation - find project",
    Prefixes.of(schema -> "schema", renku -> "renku", prov -> "prov"),
    s"""|SELECT DISTINCT ?name ?maybeParent ?visibility ?maybeDescription (GROUP_CONCAT(?keyword; separator=',') AS ?keywords)
        |WHERE {
        |  <$resourceId> a schema:Project;
        |                schema:name ?name;
        |                renku:projectVisibility ?visibility. 
        |  OPTIONAL { <$resourceId> schema:description ?maybeDescription . }
        |  OPTIONAL { <$resourceId> schema:keywords ?keyword . }
        |  OPTIONAL { <$resourceId> prov:wasDerivedFrom ?maybeParent }            
        |}
        |GROUP BY ?name ?maybeParent ?visibility ?maybeDescription
        |""".stripMargin
  )

  private implicit lazy val recordsDecoder: Decoder[List[KGProjectInfo]] = {
    val toSetOfKeywords: Option[String] => Decoder.Result[Set[projects.Keyword]] =
      _.map(_.split(',').toList.map(projects.Keyword.from).sequence.map(_.toSet)).sequence
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(_.getOrElse(Set.empty))
    val rowDecoder = Decoder.instance(cursor =>
      for {
        name             <- cursor.downField("name").downField("value").as[projects.Name]
        maybeParent      <- cursor.downField("maybeParent").downField("value").as[Option[projects.ResourceId]]
        visibility       <- cursor.downField("visibility").downField("value").as[projects.Visibility]
        maybeDescription <- cursor.downField("maybeDescription").downField("value").as[Option[projects.Description]]
        keywords         <- cursor.downField("keywords").downField("value").as[Option[String]].flatMap(toSetOfKeywords)
      } yield (name, maybeParent, visibility, maybeDescription, keywords)
    )
    _.downField("results").downField("bindings").as(decodeList(rowDecoder))
  }

  private def toSingleResult(resourceId: projects.ResourceId): List[KGProjectInfo] => F[Option[KGProjectInfo]] = {
    case Nil           => MonadThrow[F].pure(Option.empty[KGProjectInfo])
    case record +: Nil => MonadThrow[F].pure(Some(record))
    case _ => MonadThrow[F].raiseError(new Exception(s"More than one project found for resourceId: '$resourceId'"))
  }
}

private object KGProjectFinder {
  def apply[F[_]: Async: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[KGProjectFinder[F]] = for {
    config <- RdfStoreConfig[F]()
  } yield new KGProjectFinderImpl(config, timeRecorder)

  type KGProjectInfo = (projects.Name,
                        Option[projects.ResourceId],
                        projects.Visibility,
                        Option[projects.Description],
                        Set[projects.Keyword]
  )
}
