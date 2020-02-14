/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.users.{Affiliation, Email, Name => UserName}
import ch.datascience.knowledgegraph.datasets.model.DatasetCreator
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.{Result, decodeList}
import io.circe.HCursor

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private class CreatorsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder) {

  import CreatorsFinder._

  def findCreators(identifier: Identifier): IO[Set[DatasetCreator]] =
    queryExpecting[List[DatasetCreator]](using = query(identifier))
      .map(_.toSet)

  private def query(identifier: Identifier) = SparqlQuery(
    name = "ds by id - creators",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?email ?name ?affiliation
        |WHERE {
        |  ?dataset rdf:type <http://schema.org/Dataset> ;
        |           schema:identifier "$identifier" ;
        |           schema:creator ?creatorResource .
        |  OPTIONAL { ?creatorResource rdf:type <http://schema.org/Person> ;
        |                              schema:email ?email . } .
        |  OPTIONAL { ?creatorResource rdf:type <http://schema.org/Person> ;
        |                              schema:affiliation ?affiliation . } .
        |  ?creatorResource rdf:type <http://schema.org/Person> ;
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
