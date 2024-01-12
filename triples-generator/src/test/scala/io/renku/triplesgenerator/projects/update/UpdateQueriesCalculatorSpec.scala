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

package io.renku.triplesgenerator.projects.update

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.{TestSearchInfoDatasets, concatSeparator}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities, projects}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesgenerator.api.ProjectUpdates
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQuery}
import org.scalacheck.Gen
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatest.{OptionValues, Succeeded}
import org.typelevel.log4cats.Logger

class UpdateQueriesCalculatorSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with TestSearchInfoDatasets
    with OptionValues
    with should.Matchers {

  it should "create upsert queries when there's a new description" in projectsDSConfig.use { implicit pcc =>
    val project = anyProjectEntities.generateOne.to[entities.Project]

    val newValue = projectDescriptions.generateSome
    val updates  = ProjectUpdates.empty.copy(newDescription = newValue.some)

    for {
      _ <- provisionProject(project).assertNoException

      beforeUpdate = TSData(project)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe beforeUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe beforeUpdate)

      _ <- (updatesCalculator.calculateUpdateQueries(project.slug, updates) >>= execute).assertNoException

      afterUpdate = beforeUpdate.copy(maybeDescription = newValue)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe afterUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe afterUpdate)
    } yield Succeeded
  }

  it should "create delete queries when description is removed" in projectsDSConfig.use { implicit pcc =>
    val project = anyProjectEntities
      .map(replaceProjectDesc(projectDescriptions.generateSome))
      .generateOne
      .to[entities.Project]

    val newValue = Option.empty[projects.Description]
    val updates  = ProjectUpdates.empty.copy(newDescription = newValue.some)

    for {
      _ <- provisionProject(project).assertNoException

      beforeUpdate = TSData(project)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe beforeUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe beforeUpdate)

      _ <- (updatesCalculator.calculateUpdateQueries(project.slug, updates) >>= execute).assertNoException

      afterUpdate = beforeUpdate.copy(maybeDescription = newValue)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe afterUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe afterUpdate)
    } yield Succeeded
  }

  it should "create upsert queries when there are new keywords" in projectsDSConfig.use { implicit pcc =>
    val project = anyProjectEntities
      .map(replaceProjectKeywords(projectKeywords.generateSet(min = 1)))
      .generateOne
      .to[entities.Project]

    val newValue = projectKeywords.generateSet(min = 1)
    val updates  = ProjectUpdates.empty.copy(newKeywords = newValue.some)

    for {
      _ <- provisionProject(project).assertNoException

      beforeUpdate = TSData(project)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe beforeUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe beforeUpdate)

      _ <- (updatesCalculator.calculateUpdateQueries(project.slug, updates) >>= execute).assertNoException

      afterUpdate = beforeUpdate.copy(keywords = newValue)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe afterUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe afterUpdate)
    } yield Succeeded
  }

  it should "create queries that deletes keywords on removal" in projectsDSConfig.use { implicit pcc =>
    val project = anyProjectEntities
      .map(replaceProjectKeywords(projectKeywords.generateSet(min = 1)))
      .generateOne
      .to[entities.Project]

    val newValue = Set.empty[projects.Keyword]
    val updates  = ProjectUpdates.empty.copy(newKeywords = newValue.some)

    for {
      _ <- provisionProject(project).assertNoException

      beforeUpdate = TSData(project)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe beforeUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe beforeUpdate)

      _ <- (updatesCalculator.calculateUpdateQueries(project.slug, updates) >>= execute).assertNoException

      afterUpdate = beforeUpdate.copy(keywords = newValue)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe afterUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe afterUpdate)
    } yield Succeeded
  }

  it should "create upsert queries when there are new images" in projectsDSConfig.use { implicit pcc =>
    val project = anyProjectEntities.generateOne.to[entities.Project]

    val newValue = imageUris.generateList(min = 1)
    val updates  = ProjectUpdates.empty.copy(newImages = newValue.some)

    for {
      _ <- provisionProject(project).assertNoException

      beforeUpdate = TSData(project)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe beforeUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe beforeUpdate)

      _ <- (updatesCalculator.calculateUpdateQueries(project.slug, updates) >>= execute).assertNoException

      afterUpdate = beforeUpdate.copy(images = newValue)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe afterUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe afterUpdate)
    } yield Succeeded
  }

  it should "create queries that deletes images on removal" in projectsDSConfig.use { implicit pcc =>
    val project = anyProjectEntities
      .map(replaceImages(imageUris.generateList(min = 1)))
      .generateOne
      .to[entities.Project]

    val newValue = List.empty[ImageUri]
    val updates  = ProjectUpdates.empty.copy(newImages = newValue.some)

    for {
      _ <- provisionProject(project).assertNoException

      beforeUpdate = TSData(project)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe beforeUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe beforeUpdate)

      _ <- (updatesCalculator.calculateUpdateQueries(project.slug, updates) >>= execute).assertNoException

      afterUpdate = beforeUpdate.copy(images = newValue)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe afterUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe afterUpdate)
    } yield Succeeded
  }

  it should "create queries that inserts keywords when there were none" in projectsDSConfig.use { implicit pcc =>
    val project = anyProjectEntities.map(replaceProjectKeywords(Set.empty)).generateOne.to[entities.Project]

    val newValue = projectKeywords.generateSet(min = 1)
    val updates  = ProjectUpdates.empty.copy(newKeywords = newValue.some)

    for {
      _ <- provisionProject(project).assertNoException

      beforeUpdate = TSData(project)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe beforeUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe beforeUpdate)

      _ <- (updatesCalculator.calculateUpdateQueries(project.slug, updates) >>= execute).assertNoException

      afterUpdate = beforeUpdate.copy(keywords = newValue)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe afterUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe afterUpdate)
    } yield Succeeded
  }

  it should "create upsert queries when there's a visibility update" in projectsDSConfig.use { implicit pcc =>
    val project = anyProjectEntities.generateOne.to[entities.Project]

    val newValue = Gen.oneOf(projects.Visibility.all - project.visibility).generateOne
    val updates  = ProjectUpdates.empty.copy(newVisibility = newValue.some)

    for {
      _ <- provisionProject(project).assertNoException

      beforeUpdate = TSData(project)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe beforeUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe beforeUpdate)

      _ <- (updatesCalculator.calculateUpdateQueries(project.slug, updates) >>= execute).assertNoException

      afterUpdate = beforeUpdate.copy(visibility = newValue)
      _ <- dataInProjectGraph(project).asserting(_.value shouldBe afterUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe afterUpdate)
    } yield Succeeded
  }

  it should "create no upsert queries if there are no new values" in projectsDSConfig.use { implicit pcc =>
    val project      = anyProjectEntities.generateOne.to[entities.Project]
    val beforeUpdate = TSData(project)

    for {
      _ <- provisionProject(project).assertNoException

      _ <- (updatesCalculator.calculateUpdateQueries(project.slug, ProjectUpdates.empty) >>= execute).assertNoException

      _ <- dataInProjectGraph(project).asserting(_.value shouldBe beforeUpdate)
      _ <- dataInProjectsGraph(project).asserting(_.value shouldBe beforeUpdate)
    } yield Succeeded
  }

  implicit override val ioLogger: Logger[IO] = TestLogger[IO]()
  private lazy val updatesCalculator = new UpdateQueriesCalculatorImpl[IO]

  private def execute(queries: List[SparqlQuery])(implicit pcc: ProjectsConnectionConfig) =
    queries.traverse_(runUpdate)

  private case class TSData(maybeDescription: Option[projects.Description],
                            visibility:       projects.Visibility,
                            images:           List[ImageUri],
                            keywords:         Set[projects.Keyword]
  )

  private object TSData {
    def apply(project: entities.Project): TSData =
      TSData(project.maybeDescription, project.visibility, project.images.map(_.uri), project.keywords)
  }

  private def dataInProjectGraph(project: entities.Project)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Option[TSData]] =
    runSelect(
      SparqlQuery.ofUnsafe(
        "UpdateQueriesCalculator Project fetch",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT DISTINCT ?maybeDesc ?visibility
                 |  (GROUP_CONCAT(DISTINCT ?keyword; separator=$concatSeparator) AS ?keywords)
                 |  (GROUP_CONCAT(?encodedImageUrl; separator=$concatSeparator) AS ?images)
                 |WHERE {
                 |  BIND (${GraphClass.Project.id(project.resourceId)} AS ?id)
                 |  GRAPH ?id {
                 |    ?id renku:slug ?slug;
                 |        renku:projectVisibility ?visibility.
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
                 |GROUP BY ?maybeDesc ?visibility
                 |""".stripMargin
      )
    ).map(toDataExtract).flatMap(toOptionOrFail)

  private def dataInProjectsGraph(project: entities.Project)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Option[TSData]] =
    runSelect(
      SparqlQuery.ofUnsafe(
        "UpdateQueriesCalculator Projects fetch",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT DISTINCT ?maybeDesc ?visibility ?keywords ?images
                 |WHERE {
                 |  BIND (${project.resourceId.asEntityId} AS ?id)
                 |  GRAPH ${GraphClass.Projects.id} {
                 |    ?id renku:projectVisibility ?visibility.
                 |    OPTIONAL { ?id schema:description ?maybeDesc }
                 |    OPTIONAL { ?id renku:keywordsConcat ?keywords }
                 |    OPTIONAL { ?id renku:imagesConcat ?images }
                 |  }
                 |}
                 |""".stripMargin
      )
    ).map(toDataExtract).flatMap(toOptionOrFail)

  private lazy val toDataExtract: List[Map[String, String]] => List[TSData] =
    _.map { row =>
      TSData(
        row.get("maybeDesc").map(projects.Description),
        projects.Visibility(row("visibility")),
        toListOfImages(row.get("images")),
        toSetOfKeywords(row.get("keywords"))
      )
    }

  private lazy val toSetOfKeywords: Option[String] => Set[projects.Keyword] =
    _.map(_.split(concatSeparator).toList.map(projects.Keyword(_)).toSet).getOrElse(Set.empty)

  private lazy val toListOfImages: Option[String] => List[ImageUri] =
    _.map(ImageUri.fromSplitString(concatSeparator)(_).fold(throw _, identity)).getOrElse(Nil)

  private lazy val toOptionOrFail: List[TSData] => IO[Option[TSData]] = {
    case Nil      => Option.empty[TSData].pure[IO]
    case h :: Nil => h.some.pure[IO]
    case l        => new Exception(s"Found ${l.size} rows but expected not more than one").raiseError[IO, Nothing]
  }
}
