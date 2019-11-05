/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.reprovisioning.postreprovisioning

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import ch.datascience.triplesgenerator.reprovisioning.RdfStoreUpdater
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder
import io.circe.Decoder.decodeList

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait DuplicatePersonNameRemover[Interpretation[_]] extends RdfStoreUpdater[Interpretation] {
  override val description = "Removing Person's 'schema:name' if there is more than one"
}

private class IODuplicatePersonNameRemover(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger)
    with DuplicatePersonNameRemover[IO] {

  import cats.implicits._

  override def run: IO[Unit] =
    for {
      persons            <- personsWithDuplicateNames
      personsAndAllNames <- (persons map findNames).parSequence
      personAndDuplicates = personsAndAllNames map toPersonAndDuplicates
      _ <- (personAndDuplicates map deleteDuplicates).parSequence
    } yield ()

  private def personsWithDuplicateNames =
    queryExpecting[List[String]] {
      """|PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX schema: <http://schema.org/>
         |
         |SELECT (?person AS ?item)
         |WHERE {
         |  ?person rdf:type schema:Person ;
         |          schema:name ?name .
         |}
         |GROUP BY ?person
         |HAVING (COUNT(?name) > 1)
         |""".stripMargin
    }

  private def findNames(of: String): IO[(String, List[String])] =
    queryExpecting[List[String]] {
      s"""|PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          |PREFIX schema: <http://schema.org/>
          |
          |SELECT (?name AS ?item)
          |WHERE {
          |  <$of> rdf:type schema:Person ;
          |        schema:name ?name .
          |}
          |""".stripMargin
    } map (of -> _)

  private implicit lazy val itemsDecoder: Decoder[List[String]] =
    _.downField("results")
      .downField("bindings")
      .as(decodeList(_.downField("item").downField("value").as[String]))

  private lazy val toPersonAndDuplicates: ((String, List[String])) => (String, List[String]) = {
    case (person, names) =>
      names.partition(_ matches validName) match {
        case (_ +: other, rest) => person -> (other ++ rest)
        case (Nil, _ +: rest)   => person -> rest
      }
  }

  private lazy val validName = "^\\w* [\\w -]*$"

  private lazy val deleteDuplicates: ((String, List[String])) => IO[Unit] = {
    case (person, names) =>
      queryWitNoResult {
        s"""|PREFIX schema: <http://schema.org/>
            |
            |DELETE { <$person> ?type ?name }
            |WHERE {
            |  <$person> ?type ?name . 
            |  FILTER (?name IN (${names.map(name => s"'$name'").mkString(",")}))
            |}
            |""".stripMargin
      }
  }
}
