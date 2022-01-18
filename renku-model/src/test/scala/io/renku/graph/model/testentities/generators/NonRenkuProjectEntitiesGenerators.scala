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

package io.renku.graph.model.testentities
package generators

import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectCreatedDates, projectDescriptions, projectKeywords, projectPaths}
import io.renku.graph.model.projects
import io.renku.graph.model.projects.{ForksCount, Visibility}
import org.scalacheck.Gen

import java.time.Instant

trait NonRenkuProjectEntitiesGenerators {
  self: EntitiesGenerators with RenkuProjectEntitiesGenerators =>

  lazy val anyNonRenkuProjectEntities: Gen[NonRenkuProject] = Gen.oneOf(
    nonRenkuProjectEntities(anyVisibility),
    nonRenkuProjectWithParentEntities(anyVisibility)
  )

  def nonRenkuProjectEntities(
      visibilityGen:  Gen[Visibility],
      minDateCreated: projects.DateCreated = projects.DateCreated(Instant.EPOCH),
      forksCountGen:  Gen[ForksCount] = anyForksCount
  ): Gen[NonRenkuProject.WithoutParent] = for {
    path             <- projectPaths
    name             <- Gen.const(path.toName)
    maybeDescription <- projectDescriptions.toGeneratorOfOptions
    dateCreated      <- projectCreatedDates(minDateCreated.value)
    maybeCreator     <- personEntities(withGitLabId).toGeneratorOfOptions
    visibility       <- visibilityGen
    forksCount       <- forksCountGen
    keywords         <- projectKeywords.toGeneratorOfSet(minElements = 0)
    members          <- personEntities(withGitLabId).toGeneratorOfSet(minElements = 0)
  } yield NonRenkuProject.WithoutParent(path,
                                        name,
                                        maybeDescription,
                                        dateCreated,
                                        maybeCreator,
                                        visibility,
                                        forksCount,
                                        keywords,
                                        members ++ maybeCreator
  )

  def nonRenkuProjectWithParentEntities(
      visibilityGen:  Gen[Visibility],
      minDateCreated: projects.DateCreated = projects.DateCreated(Instant.EPOCH)
  ): Gen[NonRenkuProject.WithParent] = nonRenkuProjectEntities(visibilityGen, minDateCreated).map(_.forkOnce()._2)

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
