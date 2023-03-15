package io.renku.entities.search

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.SearchInfoDataset
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.{ExternalJenaForSpec, InMemoryJenaForSpec, ProjectsDataset, SparqlQuery, SparqlQueryTimeRecorder}
import org.scalatest.flatspec.AnyFlatSpec

import scala.language.reflectiveCalls

class LocalSpec
    extends AnyFlatSpec
    with IOSpec
    with InMemoryJenaForSpec
    with ExternalJenaForSpec
    with EntitiesGenerators
    with ProjectsDataset
    with SearchInfoDataset {

  implicit val ioLogger:             TestLogger[IO]              = TestLogger[IO]()
  private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()

  def createProject = {
    val project = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    upload(to = projectsDataset, project)
  }

  def writeQuery(q: SparqlQuery): Unit = {
    import fs2.io.file._


    val out = Path("/Users/ekettner/org/sdsc/files/q2.sparql")
    fs2.Stream
      .emit(q.toString)
      .through(fs2.text.utf8.encode)
      .through(Files[IO].writeAll(out))
      .compile
      .drain
      .unsafeRunSync()
  }

  it should "play with query" in {

    val criteria = Criteria()
    val query    = DatasetsQuery2.query(criteria).get
    val q = SparqlQuery.of(
      "test",
      Prefixes.of(Schemas.xsd -> "xsd", Schemas.schema -> "schema", Schemas.renku -> "renku"),
      query
    )
    println(s"---- query ----\n${q.toString}\n---- ----")
    writeQuery(q)

    val results = queryRunnerFor(projectsDataset).flatMap(_.runQuery(q)).unsafeRunSync()

    println(results)
  }
}
