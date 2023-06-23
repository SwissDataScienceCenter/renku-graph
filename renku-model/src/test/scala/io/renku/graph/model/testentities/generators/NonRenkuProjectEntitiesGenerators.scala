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

package io.renku.graph.model.testentities
package generators

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.{ForksCount, Visibility}
import org.scalacheck.Gen

import java.time.Instant

trait NonRenkuProjectEntitiesGenerators {
  self: EntitiesGenerators with RenkuProjectEntitiesGenerators =>

  lazy val anyNonRenkuProjectEntities: Gen[NonRenkuProject] = anyNonRenkuProjectEntities()

  def anyNonRenkuProjectEntities(creatorGen: Gen[Person] = personEntities(withGitLabId)): Gen[NonRenkuProject] =
    Gen.oneOf(
      nonRenkuProjectEntities(anyVisibility, creatorGen = creatorGen),
      nonRenkuProjectWithParentEntities(anyVisibility, creatorGen = creatorGen)
    )

  def nonRenkuProjectEntities(
      visibilityGen:  Gen[Visibility],
      minDateCreated: projects.DateCreated = projects.DateCreated(Instant.EPOCH),
      creatorGen:     Gen[Person] = personEntities(withGitLabId),
      forksCountGen:  Gen[ForksCount] = anyForksCount
  ): Gen[NonRenkuProject.WithoutParent] = for {
    path             <- projectPaths
    name             <- Gen.const(path.toName)
    maybeDescription <- projectDescriptions.toGeneratorOfOptions
    dateCreated      <- projectCreatedDates(minDateCreated.value)
    dateModified     <- projectModifiedDates(dateCreated.value)
    maybeCreator     <- creatorGen.toGeneratorOfOptions
    visibility       <- visibilityGen
    forksCount       <- forksCountGen
    keywords         <- projectKeywords.toGeneratorOfSet(min = 0)
    members          <- personEntities(withGitLabId).toGeneratorOfSet(min = 0)
    images           <- imageUris.toGeneratorOfList()
  } yield NonRenkuProject.WithoutParent(
    path,
    name,
    maybeDescription,
    dateCreated,
    dateModified,
    maybeCreator,
    visibility,
    forksCount,
    keywords,
    members ++ maybeCreator,
    images
  )

  def nonRenkuProjectWithParentEntities(
      visibilityGen:  Gen[Visibility],
      minDateCreated: projects.DateCreated = projects.DateCreated(Instant.EPOCH),
      creatorGen:     Gen[Person] = personEntities(withGitLabId)
  ): Gen[NonRenkuProject.WithParent] =
    nonRenkuProjectEntities(visibilityGen, minDateCreated, creatorGen = creatorGen)
      .map(_.forkOnce(creatorGen = creatorGen)._2)

  implicit class NonRenkuProjectWithParentGenFactoryOps(projectGen: Gen[NonRenkuProject.WithParent]) {
    def modify(f: NonRenkuProject.WithParent => NonRenkuProject.WithParent): Gen[NonRenkuProject.WithParent] =
      projectGen.map(f)
  }

  implicit class NonRenkuProjectWithoutParentGenFactoryOps(projectGen: Gen[NonRenkuProject.WithoutParent]) {
    def modify(f: NonRenkuProject.WithoutParent => NonRenkuProject.WithoutParent): Gen[NonRenkuProject.WithoutParent] =
      projectGen.map(f)
  }

  implicit class NonRenkuProjectGenFactoryOps(projectGen: Gen[NonRenkuProject]) {
    def modify(f: NonRenkuProject => NonRenkuProject): Gen[NonRenkuProject] = projectGen.map {
      case project: NonRenkuProject.WithoutParent => f(project)
      case project: NonRenkuProject.WithParent    => f(project)
    }
  }
}
