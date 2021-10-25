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

package io.renku.graph.acceptancetests.tooling

import cats.effect.unsafe.IORuntime
import cats.effect.{FiberIO, IO, Outcome, Spawn}
import cats.syntax.all._
import io.renku.graph.acceptancetests.data.RdfStoreData
import io.renku.graph.model.RenkuVersionPair
import io.renku.graph.model.Schemas._
import io.renku.graph.model.testentities.renkuBaseUrl
import io.renku.jsonld.EntityId
import io.renku.rdfstore.FusekiBaseUrl
import org.apache.jena.fuseki.main.FusekiServer
import org.apache.jena.rdfconnection.RDFConnectionFactory
import org.apache.lucene.store.MMapDirectory

import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

object RDFStore extends RdfStoreData {

  private val logger = TestLogger()

  private val jenaPort: Int           = 3030
  val fusekiBaseUrl:    FusekiBaseUrl = FusekiBaseUrl(s"http://localhost:$jenaPort")

  // There's a problem with restarting Jena so this whole weirdness comes due to that fact
  private class JenaInstance {

    private lazy val dataset = {
      import org.apache.jena.graph.NodeFactory
      import org.apache.jena.query.DatasetFactory
      import org.apache.jena.query.text.{EntityDefinition, TextDatasetFactory, TextIndexConfig}

      val entityDefinition: EntityDefinition = {
        val definition = new EntityDefinition("uri", "name")
        definition.setPrimaryPredicate(NodeFactory.createURI((schema / "name").show))
        definition.set("description", NodeFactory.createURI((schema / "description").show))
        definition.set("slug", NodeFactory.createURI((renku / "slug").show))
        definition.set("keywords", NodeFactory.createURI((schema / "keywords").show))
        definition
      }

      TextDatasetFactory.createLucene(
        DatasetFactory.create(),
        new MMapDirectory(Files.createTempDirectory("lucene-store-jena")),
        new TextIndexConfig(entityDefinition)
      )
    }

    lazy val connection = RDFConnectionFactory.connect(dataset)

    private val jenaFiber = new ConcurrentHashMap[FiberIO[FusekiServer], Unit]()

    def start()(implicit ioRuntime: IORuntime): IO[Unit] =
      for {
        server <- IO {
                    FusekiServer
                      .create()
                      .loopback(true)
                      .port(jenaPort)
                      .add("/renku", dataset)
                      .build
                  }
        fiber <- Spawn[IO].start(IO(server.start()))
        _     <- IO(jenaFiber.put(fiber, ()))
        _     <- logger.info("RDF store started")
      } yield ()

    def stop(): IO[Unit] = {
      connection.close()
      dataset.close()
      jenaFiber
        .keys()
        .asScala
        .toList
        .headOption
        .getOrElse(throw new Exception("No JENA instance running"))
        .join
        .flatMap {
          case Outcome.Succeeded(fuseki) => fuseki.map(_.stop()).flatTap(_ => logger.info("RDF store stopped"))
          case other                     => logger.error(s"Fuseki not started : $other")
        }
    }
  }

  private val jenaReference = new ConcurrentHashMap[JenaInstance, Unit]()

  def start(
      maybeVersionPair: Option[RenkuVersionPair] = Some(currentVersionPair)
  )(implicit ioRuntime: IORuntime): IO[Unit] =
    for {
      _ <- stop()
      newJena = new JenaInstance()
      _       = jenaReference.put(newJena, ())
      _ <- newJena.start()
      _ = maybeVersionPair
            .map { currentVersionPair =>
              jenaReference
                .keys()
                .asScala
                .toList
                .headOption
                .map {
                  _.connection.update(s"""
            INSERT DATA{
              <${EntityId
                    .of((renkuBaseUrl / "version-pair").toString)}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <${renku / "VersionPair"}> ;
                                <${renku / "cliVersion"}> '${currentVersionPair.cliVersion}' ;
                                <${renku / "schemaVersion"}> '${currentVersionPair.schemaVersion}'}""")
                }
                .getOrElse(throw new Exception("No JENA instance running"))
            }
    } yield ()

  def stop(): IO[Unit] = jenaReference.keys().asScala.toList.map(_.stop()).sequence.void

  def allTriplesCount: Int =
    jenaReference.keys.asScala.toList.headOption
      .map { jena =>
        jena.connection
          .query("""|SELECT (COUNT(*) as ?count)
                    |WHERE { ?s ?p ?o
                    |  FILTER NOT EXISTS {
                    |    ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://swissdatasciencecenter.github.io/renku-ontology#VersionPair>
                    |  }
                    |  FILTER NOT EXISTS {
                    |    ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://swissdatasciencecenter.github.io/renku-ontology#ReProvisioning>
                    |  }
                    |}
                    |""".stripMargin)
          .execSelect()
          .next()
          .get("count")
          .asLiteral()
          .getInt
      }
      .getOrElse(throw new Exception("No JENA instance running"))

  def run(query: String): Seq[Map[String, String]] =
    jenaReference
      .keys()
      .asScala
      .toList
      .headOption
      .map {
        _.connection
          .query(query)
          .execSelect()
          .asScala
          .map(row => row.varNames().asScala.map(name => name -> row.get(name).toString).toMap)
          .toList
      }
      .getOrElse(throw new Exception("No JENA instance running"))
}
