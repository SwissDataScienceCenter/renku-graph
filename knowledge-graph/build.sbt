/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

name := "knowledge-graph"

Test / fork := true

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.4.5"

libraryDependencies += "com.github.dgarijo" % "widoco" % "1.4.17"

libraryDependencies += "io.swagger.parser.v3" % "swagger-parser" % "2.1.12"

// needed by widoco only - explicitly bumped up to work with rdf4j-queryparser-sparql (triples-store-client)
// from 5.1.18 that widoco comes with
libraryDependencies += "net.sourceforge.owlapi" % "owlapi-distribution" % "5.5.0"

// needed by widoco only
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.20.0"

resolvers += "jitpack" at "https://jitpack.io"
