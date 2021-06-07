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

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect.{ConcurrentEffect, Timer}
import cats.syntax.all._
import ch.datascience.graph.Schemas._
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.users.{Affiliation, Email, Name => UserName}
import ch.datascience.knowledgegraph.datasets.model.DatasetCreator
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.circe.Decoder.{Result, decodeList}
import io.circe.HCursor
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private class CreatorsFinder[Interpretation[_]: ConcurrentEffect: Timer](
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[Interpretation],
    timeRecorder:            SparqlQueryTimeRecorder[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder) {

  import CreatorsFinder._

  def findCreators(identifier: Identifier): Interpretation[Set[DatasetCreator]] =
    queryExpecting[List[DatasetCreator]](using = query(identifier)).map(_.toSet)

  private def query(identifier: Identifier) = SparqlQuery.of(
    name = "ds by id - creators",
    Prefixes.of(schema -> "schema"),
    s"""|SELECT DISTINCT ?email ?name ?affiliation
        |WHERE {
        |  ?dataset a schema:Dataset ;
        |           schema:identifier "$identifier" ;
        |           schema:creator ?creatorResource .
        |  OPTIONAL { ?creatorResource a schema:Person ;
        |                              schema:email ?email . } .
        |  OPTIONAL { ?creatorResource a schema:Person ;
        |                              schema:affiliation ?affiliation . } .
        |  ?creatorResource a schema:Person ;
        |                   schema:name ?name .
        |}""".stripMargin
  )
}

private object CreatorsFinder {

  import io.circe.Decoder

  private[datasets] implicit val creatorsDecoder: Decoder[List[DatasetCreator]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    def extract(property: String, from: HCursor): Result[Option[String]] =
      from.downField(property).downField("value").as[Option[String]]

    val creator: Decoder[DatasetCreator] = { cursor =>
      for {
        maybeEmail       <- cursor.downField("email").downField("value").as[Option[Email]]
        name             <- cursor.downField("name").downField("value").as[UserName]
        maybeAffiliation <- extract("affiliation", from = cursor).map(blankToNone).flatMap(toOption[Affiliation])
      } yield DatasetCreator(maybeEmail, name, maybeAffiliation)
    }

    _.downField("results").downField("bindings").as(decodeList(creator))
  }
}
