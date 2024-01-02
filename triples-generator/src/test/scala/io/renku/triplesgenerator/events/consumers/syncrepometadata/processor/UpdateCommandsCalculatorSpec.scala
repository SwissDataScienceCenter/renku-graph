/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import Generators._
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.{SearchInfoDatasets, concatSeparator}
import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectNames
import io.renku.graph.model.images.{Image, ImageUri}
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities, projects}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQuery}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatest.{OptionValues, Succeeded}
import org.typelevel.log4cats.Logger

import java.time.Instant

class UpdateCommandsCalculatorSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with OptionValues
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with AsyncMockFactory {

  it should "create upsert queries when there's a new name" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    val tsData           = tsDataFrom(project)
    val glData           = glDataExtracts(project.slug).generateOne
    val maybePayloadData = payloadDataExtracts.generateOption

    val newValue = projectNames.generateOne
    givenNewValuesFinding(tsData, glData, maybePayloadData, returning = NewValues.empty.copy(maybeName = newValue.some))
    val updatedTsData = tsData.copy(name = newValue)

    for {
      _ <- provisionProject(project).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)

      _ <- (updatesCalculator.calculateUpdateCommands(tsData, glData, maybePayloadData) >>= execute).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe updatedTsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe updatedTsData)
    } yield Succeeded
  }

  it should "create RedoTransformation event if there's a change in visibility" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    val tsData           = tsDataFrom(project)
    val glData           = glDataExtracts(project.slug).generateOne
    val maybePayloadData = payloadDataExtracts.generateOption

    val newValue = projectVisibilities.generateOne
    givenNewValuesFinding(tsData,
                          glData,
                          maybePayloadData,
                          returning = NewValues.empty.copy(maybeVisibility = newValue.some)
    )

    for {
      _ <- provisionProject(project).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)

      _ <- updatesCalculator
             .calculateUpdateCommands(tsData, glData, maybePayloadData)
             .asserting(_ shouldBe List(UpdateCommand.Event(StatusChangeEvent.RedoProjectTransformation(tsData.slug))))

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)
    } yield Succeeded
  }

  it should "create upsert queries when there's a new dateModified" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    val tsData           = tsDataFrom(project)
    val glData           = glDataExtracts(project.slug).generateOne
    val maybePayloadData = payloadDataExtracts.generateOption

    val newValue = projectModifiedDates(project.dateModified.value).generateSome
    givenNewValuesFinding(tsData,
                          glData,
                          maybePayloadData,
                          returning = NewValues.empty.copy(maybeDateModified = newValue)
    )
    val updatedTsData = tsData.copy(maybeDateModified = newValue)

    for {
      _ <- provisionProject(project).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)

      _ <- (updatesCalculator.calculateUpdateCommands(tsData, glData, maybePayloadData) >>= execute).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe updatedTsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe updatedTsData)
    } yield Succeeded
  }

  it should "create upsert queries when there's a new description" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    val tsData           = tsDataFrom(project)
    val glData           = glDataExtracts(project.slug).generateOne
    val maybePayloadData = payloadDataExtracts.generateOption

    val newValue = projectDescriptions.generateSome
    givenNewValuesFinding(tsData, glData, maybePayloadData, returning = NewValues.empty.copy(maybeDesc = newValue.some))
    val updatedTsData = tsData.copy(maybeDesc = newValue)

    for {
      _ <- provisionProject(project).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)

      _ <- (updatesCalculator.calculateUpdateCommands(tsData, glData, maybePayloadData) >>= execute).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe updatedTsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe updatedTsData)
    } yield Succeeded
  }

  it should "create delete queries when description is removed" in {

    val project = anyProjectEntities
      .map(replaceProjectDesc(projectDescriptions.generateSome))
      .generateOne
      .to[entities.Project]

    val tsData           = tsDataFrom(project)
    val glData           = glDataExtracts(project.slug).generateOne
    val maybePayloadData = payloadDataExtracts.generateOption

    givenNewValuesFinding(tsData, glData, maybePayloadData, returning = NewValues.empty.copy(maybeDesc = Some(None)))
    val updatedTsData = tsData.copy(maybeDesc = None)

    for {
      _ <- provisionProject(project).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)

      _ <- (updatesCalculator.calculateUpdateCommands(tsData, glData, maybePayloadData) >>= execute).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe updatedTsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe updatedTsData)
    } yield Succeeded
  }

  it should "create upsert queries when there are new keywords" in {

    val project = anyProjectEntities
      .map(replaceProjectKeywords(projectKeywords.generateSet(min = 1)))
      .generateOne
      .to[entities.Project]

    val tsData           = tsDataFrom(project)
    val glData           = glDataExtracts(project.slug).generateOne
    val maybePayloadData = payloadDataExtracts.generateOption

    val newValue = projectKeywords.generateSet(min = 1)
    givenNewValuesFinding(tsData,
                          glData,
                          maybePayloadData,
                          returning = NewValues.empty.copy(maybeKeywords = newValue.some)
    )
    val updatedTsData = tsData.copy(keywords = newValue)

    for {
      _ <- provisionProject(project).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)

      _ <- (updatesCalculator.calculateUpdateCommands(tsData, glData, maybePayloadData) >>= execute).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe updatedTsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe updatedTsData)
    } yield Succeeded
  }

  it should "create queries that deletes keywords on removal" in {

    val project = anyProjectEntities
      .map(replaceProjectKeywords(projectKeywords.generateSet(min = 1)))
      .generateOne
      .to[entities.Project]

    val tsData           = tsDataFrom(project)
    val glData           = glDataExtracts(project.slug).generateOne
    val maybePayloadData = payloadDataExtracts.generateOption

    val newValue = Set.empty[projects.Keyword]
    givenNewValuesFinding(tsData,
                          glData,
                          maybePayloadData,
                          returning = NewValues.empty.copy(maybeKeywords = newValue.some)
    )
    val updatedTsData = tsData.copy(keywords = newValue)

    for {
      _ <- provisionProject(project).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)

      _ <- (updatesCalculator.calculateUpdateCommands(tsData, glData, maybePayloadData) >>= execute).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe updatedTsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe updatedTsData)
    } yield Succeeded
  }

  it should "create upsert queries when there are new images" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    val tsData           = tsDataFrom(project)
    val glData           = glDataExtracts(project.slug).generateOne
    val maybePayloadData = payloadDataExtracts.generateOption

    val newValue = imageUris.generateList(min = 1)
    givenNewValuesFinding(tsData,
                          glData,
                          maybePayloadData,
                          returning = NewValues.empty.copy(maybeImages = Image.projectImage(tsData.id, newValue).some)
    )
    val updatedTsData = tsData.copy(images = newValue)

    for {
      _ <- provisionProject(project).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)

      _ <- (updatesCalculator.calculateUpdateCommands(tsData, glData, maybePayloadData) >>= execute).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe updatedTsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe updatedTsData)
    } yield Succeeded
  }

  it should "create queries that deletes images on removal" in {

    val project = anyProjectEntities
      .map(replaceImages(imageUris.generateList(min = 1)))
      .generateOne
      .to[entities.Project]

    val tsData           = tsDataFrom(project)
    val glData           = glDataExtracts(project.slug).generateOne
    val maybePayloadData = payloadDataExtracts.generateOption

    val newValue = List.empty[Image]
    givenNewValuesFinding(tsData,
                          glData,
                          maybePayloadData,
                          returning = NewValues.empty.copy(maybeImages = newValue.some)
    )
    val updatedTsData = tsData.copy(images = Nil)

    for {
      _ <- provisionProject(project).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)

      _ <- (updatesCalculator.calculateUpdateCommands(tsData, glData, maybePayloadData) >>= execute).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe updatedTsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe updatedTsData)
    } yield Succeeded
  }

  it should "create queries that inserts keywords when there were none" in {

    val project = anyProjectEntities.map(replaceProjectKeywords(Set.empty)).generateOne.to[entities.Project]

    val tsData           = tsDataFrom(project)
    val glData           = glDataExtracts(project.slug).generateOne
    val maybePayloadData = payloadDataExtracts.generateOption

    val newValue = projectKeywords.generateSet(min = 1)
    givenNewValuesFinding(tsData,
                          glData,
                          maybePayloadData,
                          returning = NewValues.empty.copy(maybeKeywords = newValue.some)
    )
    val updatedTsData = tsData.copy(keywords = newValue)

    for {
      _ <- provisionProject(project).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)

      _ <- (updatesCalculator.calculateUpdateCommands(tsData, glData, maybePayloadData) >>= execute).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe updatedTsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe updatedTsData)
    } yield Succeeded
  }

  it should "create no upsert queries if there are no new values" in {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    val tsData           = tsDataFrom(project)
    val glData           = glDataExtracts(project.slug).generateOne
    val maybePayloadData = payloadDataExtracts.generateOption

    givenNewValuesFinding(tsData, glData, maybePayloadData, returning = NewValues.empty)

    for {
      _ <- provisionProject(project).assertNoException

      _ <- (updatesCalculator.calculateUpdateCommands(tsData, glData, maybePayloadData) >>= execute).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe tsData)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe tsData)
    } yield Succeeded
  }

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val newValuesCalculator = mock[NewValuesCalculator]
  private lazy val updatesCalculator   = new UpdateCommandsCalculatorImpl[IO](newValuesCalculator)

  private def execute(queries: List[UpdateCommand]) =
    queries
      .collect { case q: UpdateCommand.Sparql => q.value }
      .traverse_(runUpdate(on = projectsDataset, _))

  private def dataInProjectGraph(project: entities.Project): IO[Option[DataExtract.TS]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "UpdateCommandsCalculator Project fetch",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?id ?slug ?name ?visibility ?maybeDateModified ?maybeDesc
                 |  (GROUP_CONCAT(DISTINCT ?keyword; separator=$concatSeparator) AS ?keywords)
                 |  (GROUP_CONCAT(?encodedImageUrl; separator=$concatSeparator) AS ?images)
                 |WHERE {
                 |  BIND (${GraphClass.Project.id(project.resourceId)} AS ?id)
                 |  GRAPH ?id {
                 |    ?id renku:projectPath ?slug;
                 |        schema:name ?name;
                 |        renku:projectVisibility ?visibility.
                 |    OPTIONAL { ?id schema:dateModified ?maybeDateModified }
                 |    OPTIONAL { ?id schema:description ?maybeDesc }
                 |    OPTIONAL { ?id schema:keywords ?keyword }
                 |    OPTIONAL {
                 |      ?id schema:image ?imageId.
                 |      ?imageId schema:position ?imagePosition;
                 |               schema:contentUrl ?imageUrl.
                 |      BIND(CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
                 |    }
                 |  }
                 |}
                 |GROUP BY ?id ?slug ?name ?visibility ?maybeDateModified ?maybeDesc
                 |""".stripMargin
      )
    ).map(toDataExtract).flatMap(toOptionOrFail)

  private def dataInProjectsGraph(project: entities.Project): IO[Option[DataExtract.TS]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "UpdateCommandsCalculator Projects fetch",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?id ?slug ?name ?visibility ?maybeDateModified ?maybeDesc ?keywords ?images
                 |WHERE {
                 |  BIND (${project.resourceId.asEntityId} AS ?id)
                 |  GRAPH ${GraphClass.Projects.id} {
                 |    ?id renku:projectPath ?slug;
                 |        schema:name ?name;
                 |        renku:projectVisibility ?visibility.
                 |    OPTIONAL { ?id schema:dateModified ?maybeDateModified }
                 |    OPTIONAL { ?id schema:description ?maybeDesc }
                 |    OPTIONAL { ?id renku:keywordsConcat ?keywords }
                 |    OPTIONAL { ?id renku:imagesConcat ?images }
                 |  }
                 |}
                 |""".stripMargin
      )
    ).map(toDataExtract).flatMap(toOptionOrFail)

  private lazy val toDataExtract: List[Map[String, String]] => List[DataExtract.TS] =
    _.flatMap { row =>
      (row.get("id").map(projects.ResourceId),
       row.get("slug").map(projects.Slug),
       row.get("name").map(projects.Name),
       row.get("visibility").map(projects.Visibility),
       Some(row.get("maybeDateModified").map(Instant.parse).map(projects.DateModified)),
       Some(row.get("maybeDesc").map(projects.Description)),
       Some(toSetOfKeywords(row.get("keywords"))),
       Some(toListOfImages(row.get("images")))
      ).mapN(DataExtract.TS)
    }

  private lazy val toSetOfKeywords: Option[String] => Set[projects.Keyword] =
    _.map(_.split(concatSeparator).toList.map(projects.Keyword(_)).toSet).getOrElse(Set.empty)

  private lazy val toListOfImages: Option[String] => List[ImageUri] =
    _.map(ImageUri.fromSplitString(concatSeparator)(_).fold(throw _, identity)).getOrElse(Nil)

  private def givenNewValuesFinding(tsData:           DataExtract.TS,
                                    glData:           DataExtract.GL,
                                    maybePayloadData: Option[DataExtract.Payload],
                                    returning:        NewValues
  ) = (newValuesCalculator.findNewValues _)
    .expects(tsData, glData, maybePayloadData)
    .returning(returning)

  private lazy val toOptionOrFail: List[DataExtract.TS] => IO[Option[DataExtract.TS]] = {
    case Nil      => Option.empty[DataExtract.TS].pure[IO]
    case h :: Nil => h.some.pure[IO]
    case _        => new Exception("Found more than one row").raiseError[IO, Nothing]
  }

  implicit override val ioLogger: Logger[IO] = TestLogger[IO]()
}
