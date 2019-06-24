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

name := "triples-generator"

libraryDependencies += "ch.qos.logback"  % "logback-classic"    % "1.2.3"
libraryDependencies += "com.lihaoyi"     %% "ammonite-ops"      % "1.6.7"
libraryDependencies += "io.sentry"       % "sentry-logback"     % "1.7.22"
libraryDependencies += "org.apache.jena" % "jena-rdfconnection" % "3.12.0"
