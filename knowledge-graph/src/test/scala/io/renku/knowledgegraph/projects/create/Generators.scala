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

package io.renku.knowledgegraph.projects.create

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{httpUrls, noDashUuid, positiveInts}
import io.renku.graph.model.RenkuTinyTypeGenerators._
import io.renku.knowledgegraph.projects.images.ImageGenerators
import org.scalacheck.Gen

private object Generators {

  implicit val namespaceIds: Gen[NamespaceId] = positiveInts().map(_.value).toGeneratorOf(NamespaceId)
  implicit val templateRepositoryUrls: Gen[templates.RepositoryUrl] = httpUrls().toGeneratorOf(templates.RepositoryUrl)
  implicit val templateIdentifiers:    Gen[templates.Identifier]    = noDashUuid.toGeneratorOf(templates.Identifier)

  implicit val templatesGen: Gen[Template] =
    (templateRepositoryUrls, templateIdentifiers).mapN(Template.apply)

  implicit val newProjects: Gen[NewProject] =
    for {
      name             <- projectNames
      namespaceId      <- namespaceIds
      slug             <- projectSlugs
      maybeDescription <- projectDescriptions.toGeneratorOfOptions
      keywords         <- projectKeywords.toGeneratorOfSet()
      visibility       <- projectVisibilities
      template         <- templatesGen
      image            <- ImageGenerators.images.toGeneratorOfOptions
    } yield NewProject(name, namespaceId, slug, maybeDescription, keywords, visibility, template, image)

  implicit val glCreatedProjectsGen: Gen[GLCreatedProject] =
    imageUris.toGeneratorOfOptions.map(GLCreatedProject.apply)
}
