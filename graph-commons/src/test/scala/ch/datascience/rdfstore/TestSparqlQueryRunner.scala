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

package ch.datascience.rdfstore

import cats.effect.{ExitCode, IO, IOApp}
import ch.datascience.http.client.{BasicAuthCredentials, BasicAuthPassword, BasicAuthUsername}
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import io.circe.{Decoder, HCursor, Json}

private object TestSparqlQueryRunner extends IOApp {

  private val query: String =
    s"""|SELECT ?s ?p ?o
        |WHERE { ?s ?p ?o }
        |""".stripMargin

  import RdfStoreServer._
  import cats.syntax.all._
  import ch.datascience.graph.model.Schemas._
  import eu.timepit.refined.auto._

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.language.reflectiveCalls

  override def run(args: List[String]): IO[ExitCode] =
    queryRunner
      .runQuery(query)
      .map(printResults)
      .map(_ => ExitCode.Success)

  private def printResults(results: List[Map[String, String]]): Unit = {
    println("[")
    println(
      results.map(line => line.map { case (k, v) => s"    $k:\t$v" }.mkString("  {\n", "\n", "\n  }")).mkString(",\n")
    )
    println("]")
  }

  private lazy val logger = TestLogger[IO]()
  private lazy val queryRunner = new RdfStoreClientImpl(
    RdfStoreConfig(FusekiBaseUrl(s"http://localhost:$fusekiPort"),
                   datasetName,
                   BasicAuthCredentials(BasicAuthUsername("not-needed"), BasicAuthPassword("not-needed"))
    ),
    logger,
    new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
  ) {

    import io.circe.Decoder._

    def runQuery(query: String): IO[List[Map[String, String]]] =
      queryExpecting[List[Map[String, String]]] {
        SparqlQuery.of(
          name = "test query",
          Prefixes
            .of(
              prov   -> "prov",
              rdf    -> "rdf",
              rdfs   -> "rdfs",
              renku  -> "renku",
              schema -> "schema",
              text   -> "text"
            ),
          query
        )
      }

    def runUpdate(query: SparqlQuery): IO[Unit] = updateWithNoResult(using = query)

    private implicit lazy val valuesDecoder: Decoder[List[Map[String, String]]] = { cursor =>
      for {
        vars <- cursor.as[List[String]]
        values <- cursor
                    .downField("results")
                    .downField("bindings")
                    .as[List[Map[String, String]]](decodeList(valuesDecoder(vars)))
      } yield values
    }

    private implicit lazy val varsDecoder: Decoder[List[String]] =
      _.downField("head").downField("vars").as[List[Json]].flatMap(_.map(_.as[String]).sequence)

    private def valuesDecoder(vars: List[String]): Decoder[Map[String, String]] =
      implicit cursor =>
        vars
          .map(varToMaybeValue)
          .sequence
          .map(_.flatten)
          .map(_.toMap)

    private def varToMaybeValue(varName: String)(implicit cursor: HCursor) =
      cursor
        .downField(varName)
        .downField("value")
        .as[Option[String]]
        .map(maybeValue => maybeValue map (varName -> _))
  }
}
