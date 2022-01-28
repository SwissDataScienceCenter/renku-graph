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

package io.renku.graph.acceptancetests.tooling

import cats.data.OptionT
import cats.effect._
import cats.effect.unsafe.IORuntime
import cats.syntax.all._
import io.renku.graph.acceptancetests.data.RdfStoreData
import RdfStoreData.currentVersionPair
import io.renku.graph.model.RenkuVersionPair
import io.renku.graph.model.Schemas._
import io.renku.graph.model.testentities.renkuBaseUrl
import io.renku.jsonld.EntityId
import io.renku.rdfstore.FusekiBaseUrl
import org.apache.jena.fuseki.main.FusekiServer
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionFactory}
import org.apache.lucene.store.MMapDirectory

import java.nio.file.Files
import scala.jdk.CollectionConverters._

object RDFStore {

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

    lazy val connection: RDFConnection = RDFConnectionFactory.connect(dataset)
    private val maybeJenaReference = Ref.unsafe[IO, Option[FusekiServer]](None)

    def start(): IO[Unit] = for {
      server <- IO {
                  FusekiServer
                    .create()
                    .loopback(true)
                    .port(jenaPort)
                    .add("/renku", dataset)
                    .build
                }
      _ <- Spawn[IO].start(IO(server.start()))
      _ <- maybeJenaReference.getAndSet(server.some)
      _ <- logger.info("RDF store started")
    } yield ()

    def stop(): IO[Unit] = {
      connection.close()
      dataset.close()
      OptionT(maybeJenaReference getAndSet None)
        .map { jena => jena.stop(); logger.info("RDF store stopped") }
        .value
        .void
    }
  }

  private val maybeJenaReference = Ref.unsafe[IO, Option[JenaInstance]](None)

  def start(maybeVersionPair: Option[RenkuVersionPair] = Some(currentVersionPair)): IO[Unit] = for {
    _ <- stop()
    newJena = new JenaInstance()
    _ <- maybeJenaReference getAndSet newJena.some
    _ <- newJena.start()
    _ = maybeVersionPair.map { currentVersionPair =>
          newJena.connection.update(s"""
          INSERT DATA{
            <${EntityId.of((renkuBaseUrl / "version-pair").toString)}> a <${renku / "VersionPair"}> ;
                            <${renku / "cliVersion"}> '${currentVersionPair.cliVersion}' ;
                            <${renku / "schemaVersion"}> '${currentVersionPair.schemaVersion}'}""")
        }
  } yield ()

  def stop(): IO[Unit] = OptionT(maybeJenaReference getAndSet None).semiflatMap(_.stop()).value.void

  def allTriplesCount(implicit ioRuntime: IORuntime): Int = maybeJenaReference.get
    .map(_.getOrElse(throw new Exception("No JENA instance running")))
    .map {
      _.connection
        .query("""|SELECT (COUNT(*) as ?count)
                  |WHERE { ?s ?p ?o
                  |  FILTER NOT EXISTS {
                  |    ?s a <https://swissdatasciencecenter.github.io/renku-ontology#VersionPair>
                  |  }
                  |  FILTER NOT EXISTS {
                  |    ?s a <https://swissdatasciencecenter.github.io/renku-ontology#ReProvisioning>
                  |  }
                  |}
                  |""".stripMargin)
        .execSelect()
        .next()
        .get("count")
        .asLiteral()
        .getInt
    }
    .unsafeRunSync()

  def run(query: String)(implicit ioRuntime: IORuntime): Seq[Map[String, String]] =
    maybeJenaReference.get
      .map(_.getOrElse(throw new Exception("No JENA instance running")))
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
