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

package ch.datascience.graph.acceptancetests.tooling

import cats.effect.concurrent.MVar
import cats.effect.{ContextShift, Fiber, IO}
import ch.datascience.graph.Schemas.renku
import ch.datascience.graph.acceptancetests.data.RdfStoreData
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.graph.model.events.CommitId
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.bundles
import io.renku.jsonld.EntityId
import org.apache.jena.fuseki.main.FusekiServer
import org.apache.jena.rdfconnection.RDFConnectionFactory
import org.apache.lucene.store.MMapDirectory

import java.nio.file.Files
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

object RDFStore extends RdfStoreData {

  private val logger = TestLogger()

  private val jenaPort: Int           = 3030
  val fusekiBaseUrl:    FusekiBaseUrl = FusekiBaseUrl(s"http://localhost:$jenaPort")

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  // There's a problem with restarting Jena so this whole weirdness comes due to that fact
  private class JenaInstance {

    private lazy val dataset = {
      import org.apache.jena.graph.NodeFactory
      import org.apache.jena.query.DatasetFactory
      import org.apache.jena.query.text.{EntityDefinition, TextDatasetFactory, TextIndexConfig}

      val entityDefinition: EntityDefinition = {
        val definition = new EntityDefinition("uri", "name")
        definition.setPrimaryPredicate(NodeFactory.createURI("http://schema.org/name"))
        definition.set("description", NodeFactory.createURI("http://schema.org/description"))
        definition.set("alternateName", NodeFactory.createURI("http://schema.org/alternateName"))
        definition.set("keywords", NodeFactory.createURI("http://schema.org/keywords"))
        definition
      }

      TextDatasetFactory.createLucene(
        DatasetFactory.create(),
        new MMapDirectory(Files.createTempDirectory("lucene-store-jena")),
        new TextIndexConfig(entityDefinition)
      )
    }

    lazy val connection = RDFConnectionFactory.connect(dataset)

    private val jenaFiber = MVar.empty[IO, Fiber[IO, FusekiServer]].unsafeRunSync()

    def start(): IO[Unit] =
      for {
        _ <- contextShift.shift
        fiber <- IO {
                   FusekiServer
                     .create()
                     .loopback(true)
                     .port(jenaPort)
                     .add("/renku", dataset)
                     .build
                     .start()
                 }.start
        _ <- jenaFiber.put(fiber)
        _ <- logger.info("RDF store started")
      } yield ()

    def stop(): IO[Unit] = {
      connection.close()
      dataset.close()
      jenaFiber.tryTake.flatMap {
        case None => IO.unit
        case Some(fiber) =>
          for {
            _           <- fiber.join.map(_.stop())
            cancelToken <- fiber.cancel
            _           <- logger.info("RDF store stopped")
          } yield cancelToken
      }
    }
  }

  private val jenaReference = MVar.empty[IO, JenaInstance].unsafeRunSync()

  def start(maybeVersionPair: Option[RenkuVersionPair] = Some(currentVersionPair)): IO[Unit] =
    for {
      _ <- stop()
      newJena = new JenaInstance()
      _ <- jenaReference.put(newJena)
      _ <- newJena.start()
      _ <- maybeVersionPair
             .map { currentVersionPair =>
               jenaReference.read.map {
                 _.connection
                   .update(s"""
            INSERT DATA{
              <${EntityId
                     .of((bundles.renkuBaseUrl / "version-pair").toString)}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <${renku / "VersionPair"}> ;
                                <${renku / "cliVersion"}> '${currentVersionPair.cliVersion}' ;
                                <${renku / "schemaVersion"}> '${currentVersionPair.schemaVersion}'}""")
               }
             }
             .getOrElse(IO.unit)
    } yield ()

  def stop(): IO[Unit] =
    for {
      maybeJena <- jenaReference.tryTake
      _         <- maybeJena.map(_.stop()).getOrElse(IO.unit)
    } yield ()

  def allTriplesCount: Int =
    jenaReference.read
      .map { jena =>
        jena.connection
          .query("""SELECT (COUNT(*) as ?count) 
                                WHERE { ?s ?p ?o 
                                  FILTER NOT EXISTS {
                                    ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://swissdatasciencecenter.github.io/renku-ontology#VersionPair>
                                  }
                                  FILTER NOT EXISTS {
                                    ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://swissdatasciencecenter.github.io/renku-ontology#ReProvisioning>
                                  }
                                }
        """)
          .execSelect()
          .next()
          .get("count")
          .asLiteral()
          .getInt
      }
      .unsafeRunSync()

  def commitTriplesCount(commitId: CommitId) = jenaReference.read
    .map { jena =>
      jena.connection
        .query(s"""SELECT (COUNT(*) as ?count) 
                                WHERE { ?s  <http://www.w3.org/2000/01/rdf-schema#label> '${commitId.value}' }
        """)
        .execSelect()
        .next()
        .get("count")
        .asLiteral()
        .getInt
    }
    .unsafeRunSync()

  def run(query: String): Seq[Map[String, String]] =
    jenaReference.read
      .map {
        _.connection
          .query(query)
          .execSelect()
          .asScala
          .map(row => row.varNames().asScala.map(name => name -> row.get(name).toString).toMap)
          .toList
      }
      .unsafeRunSync()
}
