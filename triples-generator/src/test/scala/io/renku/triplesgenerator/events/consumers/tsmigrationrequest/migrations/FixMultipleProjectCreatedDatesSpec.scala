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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect.IO
import cats.syntax.all._
import io.renku.graph.model.{GraphClass, entities, projects}
import io.renku.graph.model.testentities._
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import io.renku.tinytypes.syntax.all._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger
import eu.timepit.refined.auto._
import io.renku.triplesstore.SparqlQuery.Prefixes

import java.time.Instant
import scala.concurrent.duration._

class FixMultipleProjectCreatedDatesSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  implicit val logger: Logger[IO] = TestLogger()

  "run" should {
    "remove obsolete project created dates when multiple exist" in {
      val data = anyRenkuProjectEntities
        .modify(replaceProjectCreator(personEntities.generateSome))
        .addDataset(datasetEntities(provenanceInternal))
        .generateList(min = 2, max = 2)
        .map(_.bimap(identity, _.to[entities.Project]))

      upload(to = projectsDataset, data.map(_._2): _*)
      val moreDates = List.range(1, 3)
      moreDates
        .flatMap(n => data.map(_._2).map(project => (project.resourceId, project.dateCreated - n.days)))
        .foreach { case (id, date) =>
          val graphId = GraphClass.Project.id(id)
          insert(to = projectsDataset, Quad(graphId, id, schema / "dateCreated", date))
        }

      val datesBefore = findProjectDateCreated
      datesBefore.size           shouldBe 2
      datesBefore.map(_._2.size) shouldBe List.fill(2)(1 + moreDates.size)

      runUpdate(projectsDataset, FixMultipleProjectCreatedDates.query).unsafeRunSync()

      val datesAfter = findProjectDateCreated
      datesAfter.size           shouldBe 2
      datesAfter.map(_._2.size) shouldBe List(1, 1)
    }
  }

  def findProjectDateCreated: Map[projects.ResourceId, List[Instant]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "find dates",
        Prefixes.of(schema -> "schema"),
        """
          |SELECT ?projectId ?dateCreated
          |WHERE {
          |  graph ?g {
          |    ?projectId a schema:Project;
          |       schema:dateCreated ?dateCreated
          |  }
          |}
          |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => projects.ResourceId(row("projectId")) -> Instant.parse(row("dateCreated")))
      .groupMap(_._1)(_._2)
}
