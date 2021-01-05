/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.rdfstore.entities

import ch.datascience.tinytypes.constraints.{RelativePath, RelativePathOps}
import ch.datascience.tinytypes.{RelativePathTinyType, TinyTypeFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty

sealed trait Location extends Any with RelativePathTinyType
object Location       extends TinyTypeFactory[Location](new LocationImpl(_)) with RelativePath with RelativePathOps[Location]
final class LocationImpl(override val value: String) extends AnyVal with Location

sealed trait WorkflowFile extends Any with Location

object WorkflowFile {

  def cwl(fileName:  String Refined NonEmpty): WorkflowFile = new CwlFile(s".renku/workflow/$fileName")
  def yaml(fileName: String Refined NonEmpty): WorkflowFile = new YamlFile(s".renku/workflow/$fileName")

  final class CwlFile(override val value: String) extends AnyVal with WorkflowFile
  final class YamlFile(override val value: String) extends AnyVal with WorkflowFile
}
