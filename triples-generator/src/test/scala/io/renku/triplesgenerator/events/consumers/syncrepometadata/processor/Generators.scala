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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import cats.syntax.all._
import io.renku.eventlog.api.events.Generators.redoProjectTransformationEvents
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.{imageUris, projectDescriptions, projectKeywords, projectModifiedDates, projectNames, projectPaths, projectResourceIds, projectVisibilities}
import io.renku.graph.model.{entities, projects}
import org.scalacheck.Gen

private object Generators {

  def tsDataExtracts(having: projects.Path = projectPaths.generateOne): Gen[DataExtract.TS] = for {
    id                <- projectResourceIds
    name              <- projectNames
    visibility        <- projectVisibilities
    maybeDateModified <- projectModifiedDates().toGeneratorOfOptions
    maybeDesc         <- projectDescriptions.toGeneratorOfOptions
    keywords          <- projectKeywords.toGeneratorOfSet(min = 0)
    images            <- imageUris.toGeneratorOfList(max = 1)
  } yield DataExtract.TS(id, having, name, visibility, maybeDateModified, maybeDesc, keywords, images)

  def glDataExtracts(having: projects.Path = projectPaths.generateOne): Gen[DataExtract.GL] = for {
    name           <- projectNames
    visibility     <- projectVisibilities
    updatedAt      <- projectModifiedDates().map(_.value).toGeneratorOfOptions
    lastActivityAt <- projectModifiedDates().map(_.value).toGeneratorOfOptions
    maybeDesc      <- projectDescriptions.toGeneratorOfOptions
    keywords       <- projectKeywords.toGeneratorOfSet(min = 0)
    maybeImage     <- imageUris.toGeneratorOfOptions
  } yield DataExtract.GL(having, name, visibility, updatedAt, lastActivityAt, maybeDesc, keywords, maybeImage)

  def payloadDataExtracts(having: projects.Path = projectPaths.generateOne): Gen[DataExtract.Payload] = for {
    name      <- projectNames
    maybeDesc <- projectDescriptions.toGeneratorOfOptions
    keywords  <- projectKeywords.toGeneratorOfSet(min = 0)
    imageUris <- imageUris.toGeneratorOfList()
  } yield DataExtract.Payload(having, name, maybeDesc, keywords, imageUris)

  def tsDataFrom(project: entities.Project): DataExtract.TS =
    DataExtract.TS(
      project.resourceId,
      project.path,
      project.name,
      project.visibility,
      project.dateModified.some,
      project.maybeDescription,
      project.keywords,
      project.images.map(_.uri)
    )

  def glDataFrom(data: DataExtract.TS): DataExtract.GL = {

    assert(data.images.size <= 1, "More than 1 number of images cannot be modeled in GL")

    DataExtract.GL(
      data.path,
      data.name,
      data.visibility,
      data.maybeDateModified.map(_.value),
      data.maybeDateModified.map(_.value),
      data.maybeDesc,
      data.keywords,
      data.images.headOption
    )
  }

  def payloadDataFrom(data: DataExtract.TS): DataExtract.Payload =
    DataExtract.Payload(data.path, data.name, data.maybeDesc, data.keywords, data.images)

  val sparqlUpdateCommands: Gen[UpdateCommand.Sparql] = sparqlQueries.map(UpdateCommand.Sparql)
  val eventUpdateCommands:  Gen[UpdateCommand.Event]  = redoProjectTransformationEvents.map(UpdateCommand.Event)
  val updateCommands:       Gen[UpdateCommand]        = Gen.oneOf(sparqlUpdateCommands, eventUpdateCommands)
}
