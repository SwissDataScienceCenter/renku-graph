/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration
package persondetails

import cats.MonadError
import ch.datascience.graph.Schemas._
import ch.datascience.graph.model.projects
import ch.datascience.rdfstore.JsonLDTriples
import io.circe.optics.JsonPath.root

private trait ProjectPathExtractor[Interpretation[_]] {
  def extractProjectPath(triples: JsonLDTriples): Interpretation[projects.Path]
}

private object ProjectPathExtractor {
  def apply[Interpretation[_]]()(implicit
      ME: MonadError[Interpretation, Throwable]
  ): ProjectPathExtractor[Interpretation] = new ProjectPathExtractorImpl[Interpretation]
}

private class ProjectPathExtractorImpl[Interpretation[_]](implicit ME: MonadError[Interpretation, Throwable])
    extends ProjectPathExtractor[Interpretation] {

  import cats.syntax.all._
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe.Json

  override def extractProjectPath(triples: JsonLDTriples): Interpretation[projects.Path] =
    for {
      projectJson <- findSingleProject(triples.value)
      projectPath <- convertToProjectPath(projectJson)
    } yield projectPath

  private def findSingleProject(jsonPayload: Json): Interpretation[Json] =
    root.each.json
      .getAll(jsonPayload)
      .filter(isProjectObject) match {
      case Nil                  => new Exception("No project found in the payload").raiseError[Interpretation, Json]
      case singleProject :: Nil => singleProject.pure[Interpretation]
      case _                    => new Exception("More than project found in the payload").raiseError[Interpretation, Json]
    }

  private def isProjectObject(objectJson: Json): Boolean =
    objectJson.findTypes contains (schema / "Project").toString

  private def convertToProjectPath(projectJson: Json): Interpretation[projects.Path] =
    for {
      resourceId <- idToResourceId(projectJson)
      path       <- resourceId.as[Interpretation, projects.Path]
    } yield path

  private def idToResourceId(objectJson: Json): Interpretation[projects.ResourceId] =
    objectJson
      .getId[Interpretation, projects.ResourceId]
      .getOrElseF(
        new IllegalStateException(s"No @id in the object of type ${schema / "Project"}")
          .raiseError[Interpretation, projects.ResourceId]
      )
}
