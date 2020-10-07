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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.users
import ch.datascience.rdfstore._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait KGInfoFinder[Interpretation[_]] {
  def findCreatorId(email: users.Email): Interpretation[Option[users.ResourceId]]
}

private class IOKGInfoFinder(
    rdfStoreConfig: RdfStoreConfig,
    logger:         Logger[IO],
    timeRecorder:   SparqlQueryTimeRecorder[IO]
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO],
    ME:               MonadError[IO, Throwable]
) extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with KGInfoFinder[IO] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import Decoder._
  import SparqlValueEncoder.sparqlEncode
  import ch.datascience.graph.model.users.Email
  import ch.datascience.tinytypes.json.TinyTypeDecoders._

  override def findCreatorId(email: users.Email): IO[Option[users.ResourceId]] = {
    implicit val decoder: Decoder[List[users.ResourceId]] = recordsDecoder(resourceIdDecoder)
    queryExpecting[List[users.ResourceId]](using = personIdFindingQuery(email)) flatMap toSingleResult(email)
  }

  private def personIdFindingQuery(email: Email) = SparqlQuery(
    name = "upload - personId by email",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT DISTINCT ?id
        |WHERE {
        |  ?id rdf:type <http://schema.org/Person>;
        |      schema:email "${sparqlEncode(email.value)}".
        |}
        |""".stripMargin
  )

  private def recordsDecoder[T](rowDecoder: Decoder[T]): Decoder[List[T]] =
    _.downField("results").downField("bindings").as(decodeList(rowDecoder))

  private val resourceIdDecoder: Decoder[users.ResourceId] =
    _.downField("id").downField("value").as[users.ResourceId]

  private def toSingleResult[V, O](value: V): List[O] => IO[Option[O]] = {
    case Nil           => ME.pure(None)
    case record +: Nil => ME.pure(Some(record))
    case _             => ME.raiseError(new RuntimeException(s"More than one record found for '$value'"))
  }
}

private object IOKGInfoFinder {
  def apply(
      timeRecorder:   SparqlQueryTimeRecorder[IO],
      logger:         Logger[IO],
      rdfStoreConfig: IO[RdfStoreConfig] = RdfStoreConfig[IO]()
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[KGInfoFinder[IO]] =
    for {
      config <- rdfStoreConfig
    } yield new IOKGInfoFinder(config, logger, timeRecorder)
}
