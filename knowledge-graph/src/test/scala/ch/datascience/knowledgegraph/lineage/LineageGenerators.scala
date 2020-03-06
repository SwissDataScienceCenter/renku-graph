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

package ch.datascience.knowledgegraph.lineage

import ch.datascience.generators.Generators._
import ch.datascience.knowledgegraph.lineage.model._
import eu.timepit.refined.auto._
import org.scalacheck.Gen

object LineageGenerators {

  implicit val nodeIds:       Gen[Node.Id]       = nonBlankStrings(minLength = 3) map (_.value) map Node.Id.apply
  implicit val nodeLocations: Gen[Node.Location] = relativePaths() map Node.Location.apply
  implicit val nodeLabels:    Gen[Node.Label]    = nonBlankStrings(minLength = 3) map (_.value) map Node.Label.apply
  implicit val nodeTypesSet: Gen[Set[Node.Type]] = Gen.oneOf(
    Set(
      "http://www.w3.org/ns/prov#Entity",
      "http://purl.org/wf4ever/wfprov#Artifact",
      "http://www.w3.org/ns/prov#Collection"
    ).map(Node.Type.apply),
    Set("http://www.w3.org/ns/prov#Entity", "http://purl.org/wf4ever/wfprov#Artifact").map(Node.Type.apply),
    Set("http://www.w3.org/ns/prov#Activity", "http://purl.org/wf4ever/wfprov#ProcessRun").map(Node.Type.apply)
  )

  implicit val nodes: Gen[Node] = for {
    location <- nodeLocations
    label    <- nodeLabels
    types    <- nodeTypesSet
  } yield Node(location, label, types)

  implicit val edges: Gen[Edge] = for {
    sourceNode <- nodeLocations
    targetNode <- nodeLocations
  } yield Edge(sourceNode, targetNode)
}
