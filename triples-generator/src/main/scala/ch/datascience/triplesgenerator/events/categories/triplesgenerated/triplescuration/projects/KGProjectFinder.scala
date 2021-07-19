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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects

import cats.MonadThrow
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.model.{projects, users}
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects.KGProjectFinder.KGProjectInfo
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait KGProjectFinder[Interpretation[_]] {
  def find(
      resourceId: projects.ResourceId
  ): Interpretation[Option[KGProjectInfo]]
}

private class KGProjectFinderImpl(
    rdfStoreConfig: RdfStoreConfig,
    logger:         Logger[IO],
    timeRecorder:   SparqlQueryTimeRecorder[IO]
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder)
    with KGProjectFinder[IO] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import Decoder._
  import ch.datascience.graph.model.Schemas._
  import ch.datascience.tinytypes.json.TinyTypeDecoders._

  override def find(resourceId: projects.ResourceId): IO[Option[KGProjectInfo]] =
    queryExpecting[List[KGProjectInfo]](using = query(resourceId)) flatMap toSingleResult(resourceId)

  private def query(resourceId: projects.ResourceId) = SparqlQuery.of(
    name = "transformation - find project",
    Prefixes.of(schema -> "schema", renku -> "renku", prov -> "prov"),
    s"""|SELECT DISTINCT ?name ?maybeParent ?visibility ?maybeCreator
        |WHERE {
        |  <$resourceId> a schema:Project;
        |                schema:name ?name;
        |                renku:projectVisibility ?visibility.
        |  OPTIONAL { <$resourceId> schema:creator ?maybeCreator }            
        |  OPTIONAL { <$resourceId> prov:wasDerivedFrom ?maybeParent }            
        |}
        |""".stripMargin
  )

  private implicit lazy val recordsDecoder: Decoder[List[KGProjectInfo]] = {
    val rowDecoder = Decoder.instance(cursor =>
      for {
        name         <- cursor.downField("name").downField("value").as[projects.Name]
        maybeParent  <- cursor.downField("maybeParent").downField("value").as[Option[projects.ResourceId]]
        visibility   <- cursor.downField("visibility").downField("value").as[projects.Visibility]
        maybeCreator <- cursor.downField("maybeCreator").downField("value").as[Option[users.ResourceId]]
      } yield (name, maybeParent, visibility, maybeCreator)
    )
    _.downField("results").downField("bindings").as(decodeList(rowDecoder))
  }

  private def toSingleResult(resourceId: projects.ResourceId): List[KGProjectInfo] => IO[Option[KGProjectInfo]] = {
    case Nil           => MonadThrow[IO].pure(Option.empty[KGProjectInfo])
    case record +: Nil => MonadThrow[IO].pure(Some(record))
    case _ =>
      MonadThrow[IO].raiseError(new RuntimeException(s"More than one project found for resourceId: '$resourceId'"))
  }
}

private object KGProjectFinder {
  def apply(
      timeRecorder: SparqlQueryTimeRecorder[IO],
      logger:       Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[KGProjectFinder[IO]] = for {
    config <- RdfStoreConfig[IO]()
  } yield new KGProjectFinderImpl(config, logger, timeRecorder)

  type KGProjectInfo = (projects.Name, Option[projects.ResourceId], projects.Visibility, Option[users.ResourceId])
}
