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
  ): NewValues = {
    val potentiallyNew = maybePayloadData.getOrElse(glData)
    NewValues(
      Option.when(tsData.name != potentiallyNew.name)(potentiallyNew.name),
      Option.when(tsData.visibility != glData.visibility)(glData.visibility), {
        val potentiallyNewDesc = maybePayloadData.flatMap(_.maybeDesc).orElse(glData.maybeDesc)
        if (tsData.maybeDesc != potentiallyNewDesc) Some(potentiallyNewDesc) else None
      }, {
        val potentiallyNewKeywords = maybePayloadData.map(_.keywords).getOrElse(Set.empty) match {
          case k if k.isEmpty => glData.keywords
          case k              => k
        }
        Option.when(tsData.keywords != potentiallyNewKeywords)(potentiallyNewKeywords)
      }
    )
  }
}
