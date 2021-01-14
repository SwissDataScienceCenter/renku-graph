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

package ch.datascience.triplesgenerator.events.categories.membersync

class SparqlQueryCreatorSpec {

  /*
  - one query to delete removed users and insert new ones

  Difficult situation would be if the person doesn't exist in KG yet but we need to create the link to the project
  We can fetch this information when we're fetching project members

  if there are no members of this project in KG

  go through each of the GitLab members and find
  if there's a person with this GitLab ID in KG but not linked in the project
    link them to project
  else
    create a new person + the link

  for all existing members in KG who have been removed in gitlab,
    remove link in KG
   */

}
