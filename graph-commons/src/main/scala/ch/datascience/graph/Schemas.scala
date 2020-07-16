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

package ch.datascience.graph

import io.renku.jsonld.Schema

trait Schemas {
  val prov:      Schema = Schema.from("http://www.w3.org/ns/prov", separator = "#")
  val wfprov:    Schema = Schema.from("http://purl.org/wf4ever/wfprov", separator = "#")
  val wfdesc:    Schema = Schema.from("http://purl.org/wf4ever/wfdesc", separator = "#")
  val rdf:       Schema = Schema.from("http://www.w3.org/1999/02/22-rdf-syntax-ns", separator = "#")
  val rdfs:      Schema = Schema.from("http://www.w3.org/2000/01/rdf-schema", separator = "#")
  val xmlSchema: Schema = Schema.from("http://www.w3.org/2001/XMLSchema", separator = "#")
  val schema:    Schema = Schema.from("http://schema.org")
  val renku:     Schema = Schema.from("https://swissdatasciencecenter.github.io/renku-ontology", separator = "#")
}

object Schemas extends Schemas
