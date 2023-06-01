/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.entities

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._
import io.renku.config.renku.ApiUrl
import io.renku.entities.search.Criteria.Filters.EntityType
import io.renku.entities.search.model.Entity.Workflow.WorkflowType
import io.renku.entities.search.model.{Entity, MatchingScore}
import io.renku.graph.model.entities.DiffInstances
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.graph.model.{GitLabUrl, datasets, persons, plans, projects}
import io.renku.knowledgegraph.datasets.details.RequestedDataset
import io.renku.knowledgegraph.entities.ModelEncoderSpec._
import org.http4s.Uri
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.time.Instant

class ModelEncoderSpec extends AnyFlatSpec with should.Matchers with DiffInstances with AdditionalMatchers {

  implicit val gitlabUrl:   GitLabUrl = GitLabUrl("http://gitlab.io")
  implicit val renkuApiUrl: ApiUrl    = ApiUrl("http://renku.io/api")

  it should "encode images" in {
    val absoluteUri = ImageUri("http://absolu.te/uri.png")
    val relativeUri = ImageUri("relative/uri.png")
    val images      = List(absoluteUri, relativeUri)
    val path        = projects.Path("namespace/project")
    val result      = ModelEncoders.imagesEncoder.apply(images -> path)
    val expected    = images.map(makeImageLink(path))

    result shouldBe expected.asJson
  }

  it should "encode projects" in {
    val project = Entity.Project(
      matchingScore = MatchingScore(0.55f),
      path = projects.Path("some/great/ns/my-project"),
      name = projects.Name("my-project"),
      visibility = Visibility.Public,
      date = projects.DateCreated(Instant.parse("2013-03-31T13:03:45Z")),
      maybeCreator = Some(persons.Name("John Creator")),
      keywords = List("word", "super-word").map(projects.Keyword),
      maybeDescription = Some(projects.Description("description of it")),
      images = List(ImageUri("http://absolu.te/uri.png"))
    )
    val result = ModelEncoders.projectEncoder.apply(project)
    val expected = JsonProject(
      List(Href("details", s"${renkuApiUrl}/projects/${project.path.value}")),
      project.maybeDescription,
      project.maybeCreator,
      project.matchingScore,
      project.path,
      project.name,
      project.path.toNamespace,
      makeNamespaces(project.path),
      project.visibility,
      project.date,
      project.keywords,
      project.images.map(makeImageLink(project.path))
    )
    result shouldBe expected.asJson
  }

  it should "encode datasets with topmost-sameas" in {
    val dataset = Entity.Dataset(
      MatchingScore(0.65f),
      Right(datasets.TopmostSameAs(s"${renkuApiUrl}/datasets/123")),
      datasets.Name("my-dataset"),
      Visibility.Public,
      datasets.DateCreated(Instant.parse("2013-03-31T13:03:45Z")),
      List(persons.Name("John Creator")),
      List("ds-word", "word two").map(datasets.Keyword),
      Some(datasets.Description("hello description")),
      List(ImageUri("http://absolu.te/uri.png")),
      projects.Path("projx/my-project")
    )
    val result     = ModelEncoders.datasetEncoder.apply(dataset)
    val sameAs     = datasets.SameAs(dataset.sameAs.toOption.get.value)
    val detailsUri = Uri.unsafeFromString(renkuApiUrl.value) / "datasets" / RequestedDataset(sameAs)
    val expected = JsonDataset(
      List(Href("details", detailsUri.renderString)),
      dataset.matchingScore,
      dataset.name,
      dataset.name,
      dataset.visibility,
      dataset.date,
      dataset.creators,
      dataset.keywords,
      dataset.maybeDescription,
      dataset.images.map(makeImageLink(dataset.exemplarProjectPath))
    )
    result shouldBe expected.asJson
  }

  it should "encode datasets with identifier" in {
    val dataset = Entity.Dataset(
      MatchingScore(0.65f),
      Left(datasets.Identifier("123456")),
      datasets.Name("my-dataset"),
      Visibility.Public,
      datasets.DateCreated(Instant.parse("2013-03-31T13:03:45Z")),
      List(persons.Name("John Creator")),
      List("ds-word", "word two").map(datasets.Keyword),
      Some(datasets.Description("hello description")),
      List(ImageUri("http://absolu.te/uri.png")),
      projects.Path("projx/my-project")
    )
    val result     = ModelEncoders.datasetEncoder.apply(dataset)
    val identifier = dataset.sameAs.left.toOption.get
    val detailsUri = Uri.unsafeFromString(renkuApiUrl.value) / "datasets" / RequestedDataset(identifier)
    val expected = JsonDataset(
      List(Href("details", detailsUri.renderString)),
      dataset.matchingScore,
      dataset.name,
      dataset.name,
      dataset.visibility,
      dataset.date,
      dataset.creators,
      dataset.keywords,
      dataset.maybeDescription,
      dataset.images.map(makeImageLink(dataset.exemplarProjectPath))
    )
    result shouldBe expected.asJson
  }

  it should "encode workflows" in {
    val workflow = Entity.Workflow(
      MatchingScore(1.15f),
      plans.Name("my-plan"),
      Visibility.Public,
      plans.DateCreated(Instant.parse("2014-06-19T14:53:14Z")),
      List("plan-word", "another-word").map(plans.Keyword),
      Some(plans.Description("plan description")),
      WorkflowType.Step
    )
    val result = ModelEncoders.workflowEncoder.apply(workflow)
    val expected = JsonWorkflow(
      workflow.maybeDescription,
      workflow.matchingScore,
      workflow.name,
      workflow.visibility,
      workflow.date,
      workflow.keywords,
      workflow.workflowType
    )
    result shouldBe expected.asJson
  }

  it should "encode persons" in {
    val person   = Entity.Person(MatchingScore(1.4f), persons.Name("Hugo Person"))
    val result   = ModelEncoders.personEncoder.apply(person)
    val expected = JsonPerson(person.matchingScore, person.name)
    result shouldBe expected.asJson
  }

  def makeImageLink(path: projects.Path)(uri: ImageUri): ImageLink =
    uri match {
      case ImageUri.Relative(v) => ImageLink(List(Href("view", s"${gitlabUrl.value}/${path.value}/raw/master/$v")), v)
      case ImageUri.Absolute(v) => ImageLink(List(Href("view", v)), v)
    }

  def makeNamespaces(path: projects.Path): List[Ns] = {
    val parts = path.toNamespaces
    val first = Ns(parts.head.value, parts.head.value)
    parts.tail
      .foldLeft(List(first)) { (result, element) =>
        Ns(element.value, s"${first.namespace}/${element.value}") :: result
      }
      .reverse
  }
}

object ModelEncoderSpec {
  case class Href(rel: String, href: String)
  case class ImageLink(_links: List[Href], location: String)
  case class Ns(rel: String, namespace: String)
  case class JsonProject(
      _links:        List[Href],
      description:   Option[projects.Description],
      creator:       Option[persons.Name],
      matchingScore: MatchingScore,
      path:          projects.Path,
      name:          projects.Name,
      namespace:     projects.Namespace,
      namespaces:    List[Ns],
      visibility:    Visibility,
      date:          projects.DateCreated,
      keywords:      List[projects.Keyword],
      images:        List[ImageLink],
      `type`:        EntityType = EntityType.Project
  )
  case class JsonDataset(
      _links:        List[Href],
      matchingScore: MatchingScore,
      name:          datasets.Name,
      slug:          datasets.Name,
      visibility:    Visibility,
      date:          datasets.CreatedOrPublished,
      creators:      List[persons.Name],
      keywords:      List[datasets.Keyword],
      description:   Option[datasets.Description],
      images:        List[ImageLink],
      `type`:        EntityType = EntityType.Dataset
  )
  case class JsonWorkflow(
      description:   Option[plans.Description],
      matchingScore: MatchingScore,
      name:          plans.Name,
      visibility:    Visibility,
      date:          plans.DateCreated,
      keywords:      List[plans.Keyword],
      workflowType:  WorkflowType,
      `type`:        EntityType = EntityType.Workflow
  )
  case class JsonPerson(
      matchingScore: MatchingScore,
      name:          persons.Name,
      `type`:        EntityType = EntityType.Person
  )

  implicit val hrefEncoder: Encoder[Href] =
    deriveEncoder[Href]

  implicit val imageLinkEncoder: Encoder[ImageLink] =
    deriveEncoder[ImageLink]

  implicit val nsEncoder: Encoder[Ns] =
    deriveEncoder[Ns]

  implicit val projectEncoder: Encoder[JsonProject] =
    deriveEncoder[JsonProject]

  implicit val datasetEncoder: Encoder[JsonDataset] =
    deriveEncoder[JsonDataset]

  implicit val workflowEncoder: Encoder[JsonWorkflow] =
    deriveEncoder[JsonWorkflow]

  implicit val personEncoder: Encoder[JsonPerson] =
    deriveEncoder[JsonPerson]
}
