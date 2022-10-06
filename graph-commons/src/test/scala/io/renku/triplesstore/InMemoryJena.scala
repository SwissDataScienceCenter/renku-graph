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

package io.renku.triplesstore

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.syntax.all._
import com.dimafeng.testcontainers.{FixedHostPortGenericContainer, GenericContainer, SingleContainer}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.circe.{Decoder, HCursor, Json}
import io.renku.graph.model.entities.{EntityFunctions, Person}
import io.renku.graph.model.{GitLabApiUrl, GraphClass, RenkuUrl, projects, testentities}
import io.renku.graph.triplesstore.DatasetTTLs._
import io.renku.http.client._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.JsonLD.{JsonLDArray, JsonLDEntityLike}
import io.renku.jsonld._
import io.renku.logging.TestSparqlQueryTimeRecorder
import org.testcontainers.containers
import org.testcontainers.containers.wait.strategy.Wait

import scala.collection.mutable
import scala.language.reflectiveCalls

trait InMemoryJena {

  protected val jenaRunMode: JenaRunMode = JenaRunMode.GenericContainer

  private val adminCredentials = BasicAuthCredentials(BasicAuthUsername("admin"), BasicAuthPassword("admin"))

  lazy val container: SingleContainer[_] = jenaRunMode match {
    case JenaRunMode.GenericContainer =>
      GenericContainer(
        dockerImage = "renku/renku-jena:0.0.17",
        exposedPorts = Seq(3030),
        waitStrategy = Wait forHttp "/$/ping"
      )
    case JenaRunMode.FixedPortContainer(fixedPort) =>
      FixedHostPortGenericContainer(
        imageName = "renku/renku-jena:0.0.17",
        exposedPorts = Seq(3030),
        exposedHostPort = fixedPort,
        exposedContainerPort = fixedPort,
        waitStrategy = Wait forHttp "/$/ping"
      )
    case JenaRunMode.Local(_) =>
      new GenericContainer(new containers.GenericContainer("") {
        override def start(): Unit = ()
        override def stop():  Unit = ()
      })
  }

  private lazy val fusekiServerPort: Int Refined Positive = jenaRunMode match {
    case JenaRunMode.GenericContainer         => Refined.unsafeApply(container.mappedPort(container.exposedPorts.head))
    case JenaRunMode.FixedPortContainer(port) => port
    case JenaRunMode.Local(port)              => port
  }

  lazy val fusekiUrl: FusekiUrl = FusekiUrl(s"http://localhost:$fusekiServerPort")

  private val datasets: mutable.Map[FusekiUrl => DatasetConnectionConfig, DatasetConfigFile] = mutable.Map.empty

  protected def registerDataset(connectionInfoFactory: FusekiUrl => DatasetConnectionConfig,
                                maybeConfigFile:       Either[Exception, DatasetConfigFile]
  ): Unit = maybeConfigFile
    .map(configFile => datasets.addOne(connectionInfoFactory -> configFile))
    .fold(throw _, _ => ())

  protected def createDatasets(): IO[Unit] =
    datasets
      .map { case (_, configFile) => datasetsCreator.createDataset(configFile) }
      .toList
      .sequence
      .void

  def clearAllDatasets()(implicit ioRuntime: IORuntime): Unit =
    datasets
      .map { case (connectionInfoFactory, _) => connectionInfoFactory(fusekiUrl).datasetName }
      .foreach(clear)

  def clear(dataset: DatasetName)(implicit ioRuntime: IORuntime): Unit =
    queryRunnerFor(dataset)
      .runUpdate(
        SparqlQuery.of("delete all data", "CLEAR ALL")
      )
      .unsafeRunSync()

  def upload(to: DatasetName, graphs: Graph*)(implicit ioRuntime: IORuntime): Unit =
    graphs
      .map(_.flatten.fold(throw _, identity))
      .toList
      .map(queryRunnerFor(to).uploadPayload(_))
      .sequence
      .void
      .unsafeRunSync()

  def upload[T](to:    DatasetName, objects: T*)(implicit
      entityFunctions: EntityFunctions[T],
      graphsProducer:  GraphsProducer[T],
      ioRuntime:       IORuntime
  ): Unit = upload(to, objects >>= graphsProducer.apply: _*)

  def runSelect(on: DatasetName, query: SparqlQuery): IO[List[Map[String, String]]] =
    queryRunnerFor(on).runQuery(query)

  def runUpdate(on: DatasetName, query: SparqlQuery): IO[Unit] =
    queryRunnerFor(on).runUpdate(query)

  def triplesCount(on: DatasetName)(implicit ioRuntime: IORuntime): Long =
    queryRunnerFor(on)
      .runQuery(
        SparqlQuery.of("triples count", "SELECT (COUNT(?s) AS ?count) WHERE { ?s ?p ?o }")
      )
      .map(_.headOption.map(_.apply("count")).flatMap(_.toLongOption).getOrElse(0L))
      .unsafeRunSync()

  implicit class QueriesOps(queries: List[SparqlQuery]) {
    def runAll(on: DatasetName): IO[Unit] = {
      val runner = queryRunnerFor(on)
      queries.map(runner.runUpdate).sequence.void
    }
  }

  private def findConnectionInfo(datasetName: DatasetName): DatasetConnectionConfig = datasets
    .map { case (connectionInfoFactory, _) => connectionInfoFactory(fusekiUrl) }
    .find(_.datasetName == datasetName)
    .getOrElse(throw new Exception(s"Dataset '$datasetName' not registered in Test Jena instance"))

  private implicit lazy val logger:  TestLogger[IO]              = TestLogger[IO]()
  private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]

  private lazy val datasetsCreator = TSAdminClient[IO](AdminConnectionConfig(fusekiUrl, adminCredentials))

  protected def queryRunnerFor(datasetName: DatasetName) = queryRunner(findConnectionInfo(datasetName))

  private def queryRunner(connectionInfo: DatasetConnectionConfig) = new TSClientImpl[IO](connectionInfo) {

    import io.circe.Decoder._

    def uploadPayload(jsonLD: JsonLD) = upload(jsonLD)

    def runQuery(query: SparqlQuery): IO[List[Map[String, String]]] =
      queryExpecting[List[Map[String, String]]](query)

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

sealed trait DefaultGraphDataset {
  self: InMemoryJena =>

  def insert(to: DatasetName, triple: Triple)(implicit ioRuntime: IORuntime): Unit = queryRunnerFor(to)
    .runUpdate {
      SparqlQuery.of("insert triple", show"INSERT DATA { $triple }")
    }
    .unsafeRunSync()

  def delete(from: DatasetName, triple: Triple)(implicit ioRuntime: IORuntime): Unit = queryRunnerFor(from)
    .runUpdate {
      SparqlQuery.of("delete triple", show"DELETE DATA { $triple }")
    }
    .unsafeRunSync()
}

sealed trait NamedGraphDataset {
  self: InMemoryJena =>

  def delete(from: DatasetName, quad: Quad)(implicit ioRuntime: IORuntime): Unit = queryRunnerFor(from)
    .runUpdate {
      SparqlQuery.of("delete quad", show"DELETE DATA { $quad }")
    }
    .unsafeRunSync()

  def insert(to: DatasetName, quad: Quad)(implicit ioRuntime: IORuntime): Unit = queryRunnerFor(to)
    .runUpdate {
      SparqlQuery.of("insert quad", show"INSERT DATA { $quad }")
    }
    .unsafeRunSync()
}

trait GraphsProducer[T] {
  def apply(obj: T)(implicit entityFunctions: EntityFunctions[T]): List[Graph]
}

trait JenaDataset { self: InMemoryJena => }

trait RenkuDataset extends JenaDataset with DefaultGraphDataset {
  self: InMemoryJena =>

  private lazy val configFile: Either[Exception, DatasetConfigFile] = RenkuTTL.fromTtlFile()
  private lazy val connectionInfoFactory: FusekiUrl => RenkuConnectionConfig = RenkuConnectionConfig(
    _,
    BasicAuthCredentials(BasicAuthUsername("renku"), BasicAuthPassword("renku"))
  )

  def renkuDataset:          DatasetName           = renkuDSConnectionInfo.datasetName
  def renkuDSConnectionInfo: RenkuConnectionConfig = connectionInfoFactory(fusekiUrl)

  registerDataset(connectionInfoFactory, configFile)

  protected implicit val graph: GraphClass = GraphClass.Default

  protected implicit def renkuDSGraphsProducer[A](implicit
      renkuUrl: RenkuUrl,
      glApiUrl: GitLabApiUrl
  ): GraphsProducer[A] = new GraphsProducer[A] {
    import io.renku.jsonld.DefaultGraph
    import io.renku.jsonld.syntax._

    override def apply(entity: A)(implicit entityFunctions: EntityFunctions[A]): List[Graph] = {
      implicit val enc: JsonLDEncoder[A] = entityFunctions.encoder(GraphClass.Default)
      List(DefaultGraph.fromJsonLDsUnsafe(entity.asJsonLD))
    }
  }
}

trait ProjectsDataset extends JenaDataset with NamedGraphDataset {
  self: InMemoryJena =>

  private lazy val configFile: Either[Exception, DatasetConfigFile] = ProjectsTTL.fromTtlFile()
  private lazy val connectionInfoFactory: FusekiUrl => ProjectsConnectionConfig = ProjectsConnectionConfig(
    _,
    BasicAuthCredentials(BasicAuthUsername("renku"), BasicAuthPassword("renku"))
  )

  def projectsDSConnectionInfo: ProjectsConnectionConfig = connectionInfoFactory(fusekiUrl)
  def projectsDataset:          DatasetName              = projectsDSConnectionInfo.datasetName

  registerDataset(connectionInfoFactory, configFile)

  import io.renku.generators.Generators.Implicits._
  import io.renku.graph.model.GraphModelGenerators.projectPaths
  import io.renku.graph.model.projects.Path

  private lazy val defaultProjectForGraph: Path = projectPaths.generateOne

  def defaultProjectGraphId(implicit renkuUrl: RenkuUrl): EntityId =
    io.renku.graph.model.testentities.Project.toEntityId(defaultProjectForGraph)

  protected implicit def projectsDSGraphsProducer[A](implicit
      renkuUrl: RenkuUrl,
      glApiUrl: GitLabApiUrl
  ): GraphsProducer[A] = new GraphsProducer[A] {
    import io.renku.graph.model.entities
    import io.renku.jsonld.NamedGraph
    import io.renku.jsonld.syntax._

    override def apply(entity: A)(implicit entityFunctions: EntityFunctions[A]): List[Graph] =
      List(maybeBuildProjectGraph(entity), maybeBuildPersonsGraph(entity)).flatten

    private def maybeBuildProjectGraph(entity: A)(implicit entityFunctions: EntityFunctions[A]) = {
      implicit val projectEnc: JsonLDEncoder[A] = entityFunctions.encoder(GraphClass.Project)
      entity.asJsonLD match {
        case jsonLD: JsonLDEntityLike => NamedGraph(projectGraphId(entity), jsonLD).some
        case jsonLD: JsonLDArray      => NamedGraph.fromJsonLDsUnsafe(projectGraphId(entity), jsonLD).some
        case _ => None
      }
    }

    private def maybeBuildPersonsGraph(entity: A)(implicit entityFunctions: EntityFunctions[A]) = {
      implicit val graph: GraphClass = GraphClass.Persons
      entityFunctions.findAllPersons(entity).toList.map(_.asJsonLD) match {
        case Nil    => None
        case h :: t => NamedGraph.fromJsonLDsUnsafe(GraphClass.Persons.id, h, t: _*).some
      }
    }

    private def projectGraphId(entity: A): EntityId = entity match {
      case p: entities.Project     => GraphClass.Project.id(p.resourceId)
      case p: testentities.Project => GraphClass.Project.id(projects.ResourceId(p.asEntityId))
      case _ => defaultProjectGraphId
    }
  }
}

trait MigrationsDataset extends JenaDataset with DefaultGraphDataset {
  self: InMemoryJena =>

  private lazy val configFile: Either[Exception, MigrationsTTL] = MigrationsTTL.fromTtlFile()
  private lazy val connectionInfoFactory: FusekiUrl => MigrationsConnectionConfig = MigrationsConnectionConfig(
    _,
    BasicAuthCredentials(BasicAuthUsername("admin"), BasicAuthPassword("admin"))
  )

  def migrationsDSConnectionInfo: MigrationsConnectionConfig = connectionInfoFactory(fusekiUrl)
  def migrationsDataset:          DatasetName                = migrationsDSConnectionInfo.datasetName

  registerDataset(connectionInfoFactory, configFile)

  implicit def entityFunctions[A](implicit entityEncoder: JsonLDEncoder[A]): EntityFunctions[A] =
    new EntityFunctions[A] {
      override val findAllPersons: A => Set[Person]               = _ => Set.empty
      override val encoder:        GraphClass => JsonLDEncoder[A] = _ => entityEncoder
    }

  protected implicit def graphsProducer[A]: GraphsProducer[A] = new GraphsProducer[A] {

    import io.renku.jsonld.DefaultGraph
    import io.renku.jsonld.syntax._

    override def apply(entity: A)(implicit entityFunctions: EntityFunctions[A]): List[Graph] = {
      implicit val enc: JsonLDEncoder[A] = entityFunctions.encoder(GraphClass.Default)
      List(DefaultGraph.fromJsonLDsUnsafe(entity.asJsonLD))
    }
  }
}
