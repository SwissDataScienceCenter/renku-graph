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
import io.renku.graph.model.images.Image

private trait NewValuesCalculator {
  def findNewValues(tsData:           DataExtract.TS,
                    glData:           DataExtract.GL,
                    maybePayloadData: Option[DataExtract.Payload]
  ): NewValues
}

private object NewValuesCalculator extends NewValuesCalculator {

  override def findNewValues(tsData:           DataExtract.TS,
                             glData:           DataExtract.GL,
                             maybePayloadData: Option[DataExtract.Payload]
  ): NewValues = NewValues(
    maybeNewName(tsData, glData, maybePayloadData),
    Option.when(tsData.visibility != glData.visibility)(glData.visibility),
    maybeNewDateModified(tsData, glData),
    maybeNewDesc(tsData, glData, maybePayloadData),
    maybeNewKeywords(tsData, glData, maybePayloadData),
    maybeNewImages(tsData, glData, maybePayloadData)
  )

  private def maybeNewName(tsData:           DataExtract.TS,
                           glData:           DataExtract.GL,
                           maybePayloadData: Option[DataExtract.Payload]
  ) = {
    val potentiallyNewName = maybePayloadData.getOrElse(glData).name
    Option.when(tsData.name != potentiallyNewName)(potentiallyNewName)
  }

  private def maybeNewDateModified(tsData: DataExtract.TS, glData: DataExtract.GL) =
    tsData.maybeDateModified -> glData.maybeDateModified match {
      case Some(tsDate) -> Some(glDate) if tsDate < glDate => glDate.some
      case None -> Some(glDate)                            => glDate.some
      case _                                               => None
    }

  private def maybeNewDesc(tsData:           DataExtract.TS,
                           glData:           DataExtract.GL,
                           maybePayloadData: Option[DataExtract.Payload]
  ) = {
    val potentiallyNewDesc = maybePayloadData.flatMap(_.maybeDesc).orElse(glData.maybeDesc)
    if (tsData.maybeDesc != potentiallyNewDesc) Some(potentiallyNewDesc) else None
  }

  private def maybeNewKeywords(tsData:           DataExtract.TS,
                               glData:           DataExtract.GL,
                               maybePayloadData: Option[DataExtract.Payload]
  ) = {
    val potentiallyNewKeywords = maybePayloadData.map(_.keywords).getOrElse(Set.empty) match {
      case k if k.isEmpty => glData.keywords
      case k              => k
    }
    Option.when(tsData.keywords != potentiallyNewKeywords)(potentiallyNewKeywords)
  }

  private def maybeNewImages(tsData:           DataExtract.TS,
                             glData:           DataExtract.GL,
                             maybePayloadData: Option[DataExtract.Payload]
  ) = {
    val potentiallyNewImages = maybePayloadData.map(_.images).getOrElse(Nil) match {
      case Nil    => glData.maybeImage.toList
      case images => images
    }
    Option.when(tsData.images != potentiallyNewImages)(Image.projectImage(tsData.id, potentiallyNewImages))
  }
}
