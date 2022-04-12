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

package io.renku.knowledgegraph.datasets.rest

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeList
import io.renku.graph.model.Schemas._
import io.renku.graph.model.datasets._
import io.renku.graph.model.persons.{Affiliation, Email, Name => UserName}
import io.renku.knowledgegraph.datasets.model.DatasetCreator
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait CreatorsFinder[F[_]] {
  def findCreators(identifier: Identifier): F[Set[DatasetCreator]]
}

private class CreatorsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    rdfStoreConfig: RdfStoreConfig
) extends RdfStoreClientImpl(rdfStoreConfig)
    with CreatorsFinder[F] {

  import CreatorsFinder._

  def findCreators(identifier: Identifier): F[Set[DatasetCreator]] =
    queryExpecting[List[DatasetCreator]](using = query(identifier)).map(_.toSet)

  private def query(identifier: Identifier) = SparqlQuery.of(
    name = "ds by id - creators",
    Prefixes.of(schema -> "schema"),
    s"""|SELECT DISTINCT ?email ?name ?affiliation
        |WHERE {
        |  ?dataset a schema:Dataset ;
        |           schema:identifier '$identifier' ;
        |           schema:creator ?creatorResource .
        |  OPTIONAL { ?creatorResource a schema:Person ;
        |                              schema:email ?email . } .
        |  OPTIONAL { ?creatorResource a schema:Person ;
        |                              schema:affiliation ?affiliation . } .
        |  ?creatorResource a schema:Person ;
        |                   schema:name ?name .
        |}
        |""".stripMargin
  )
}

private object CreatorsFinder {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](rdfStoreConfig: RdfStoreConfig): F[CreatorsFinder[F]] =
    MonadThrow[F].catchNonFatal(new CreatorsFinderImpl(rdfStoreConfig))

  import io.circe.Decoder

  private[datasets] implicit val creatorsDecoder: Decoder[List[DatasetCreator]] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val creator: Decoder[DatasetCreator] = { cursor =>
      for {
        maybeEmail       <- cursor.downField("email").downField("value").as[Option[Email]]
        name             <- cursor.downField("name").downField("value").as[UserName]
        maybeAffiliation <- cursor.downField("affiliation").downField("value").as[Option[Affiliation]]
      } yield DatasetCreator(maybeEmail, name, maybeAffiliation)
    }

    _.downField("results").downField("bindings").as(decodeList(creator))
  }
}
