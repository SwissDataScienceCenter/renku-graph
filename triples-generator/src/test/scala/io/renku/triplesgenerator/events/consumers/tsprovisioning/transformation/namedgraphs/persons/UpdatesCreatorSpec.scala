/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.persons

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.entities.Person
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities}
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQuery}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class UpdatesCreatorSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset {

  import UpdatesCreator._

  "preparePreDataUpdates" should {

    "generate queries which delete person's name, email and affiliation " +
      "in case all of them were changed" in {

        val Some(kgPerson) = personEntities(withGitLabId, withEmail)
          .map(_.copy(maybeAffiliation = personAffiliations.generateSome))
          .generateOne
          .toMaybe[entities.Person.WithGitLabId]
        val mergedPerson = kgPerson.copy(name = personNames.generateOne,
                                         maybeEmail = personEmails.generateSome,
                                         maybeAffiliation = personAffiliations.generateSome
        )

        upload(to = projectsDataset, kgPerson)

        findPersons shouldBe Set(
          (kgPerson.resourceId.value,
           kgPerson.name.value.some,
           kgPerson.maybeEmail.map(_.value),
           kgPerson.maybeAffiliation.map(_.value),
           kgPerson.gitLabId.value.some
          )
        )

        val queries = preparePreDataUpdates(kgPerson, mergedPerson)

        queries.runAll(on = projectsDataset).unsafeRunSync()

        findPersons shouldBe Set((kgPerson.resourceId.value, None, None, None, kgPerson.gitLabId.value.some))
      }

    "generate queries which delete person's name, email and affiliation " +
      "in case they are removed" in {

        val Some(kgPerson) = personEntities(withGitLabId, withEmail)
          .map(_.copy(maybeAffiliation = personAffiliations.generateSome))
          .generateOne
          .toMaybe[entities.Person.WithGitLabId]
        val mergedPerson = kgPerson.copy(maybeEmail = None, maybeAffiliation = None)

        upload(to = projectsDataset, kgPerson)

        findPersons shouldBe Set(
          (kgPerson.resourceId.value,
           kgPerson.name.value.some,
           kgPerson.maybeEmail.map(_.value),
           kgPerson.maybeAffiliation.map(_.value),
           kgPerson.gitLabId.value.some
          )
        )

        val queries = preparePreDataUpdates(kgPerson, mergedPerson)

        queries.runAll(on = projectsDataset).unsafeRunSync()

        findPersons shouldBe Set(
          (kgPerson.resourceId.value, Some(kgPerson.name.value), None, None, kgPerson.gitLabId.value.some)
        )
      }

    "generate no queries when person's name, email and affiliation are the same" in {

      val kgPerson = personEntities(withGitLabId, withEmail)
        .map(_.copy(maybeAffiliation = personAffiliations.generateSome))
        .generateOne
        .to[entities.Person]

      preparePreDataUpdates(kgPerson, kgPerson).isEmpty shouldBe true
    }
  }

  "preparePostDataUpdates" should {

    "generate queries which delete person's duplicate name, email and/or affiliation" in {

      val Some(person) = personEntities(withGitLabId, withEmail)
        .map(_.copy(maybeAffiliation = personAffiliations.generateSome))
        .generateOne
        .toMaybe[entities.Person.WithGitLabId]
      val duplicatePerson = person.copy(name = personNames.generateOne,
                                        maybeEmail = personEmails.generateSome,
                                        maybeAffiliation = personAffiliations.generateSome
      )

      upload(to = projectsDataset, person, duplicatePerson)

      findPersons.toIdAndPropertyPairs shouldBe Set(
        (person.resourceId.value -> person.name.value).some,
        person.maybeEmail.map(person.resourceId.value -> _.value),
        person.maybeAffiliation.map(person.resourceId.value -> _.value),
        (person.resourceId.value          -> person.gitLabId.value.toString).some,
        (duplicatePerson.resourceId.value -> duplicatePerson.name.value).some,
        duplicatePerson.maybeEmail.map(duplicatePerson.resourceId.value -> _.value),
        duplicatePerson.maybeAffiliation.map(duplicatePerson.resourceId.value -> _.value),
        (duplicatePerson.resourceId.value -> duplicatePerson.gitLabId.value.toString).some
      ).flatten

      val queries = preparePostDataUpdates(person)

      queries.runAll(on = projectsDataset).unsafeRunSync()

      findPersons shouldBe Set(
        (person.resourceId.value,
         person.name.value.some,
         person.maybeEmail.map(_.value),
         person.maybeAffiliation.map(_.value),
         person.gitLabId.value.some
        )
      )
    }

    "generate queries which do nothing if there are no duplicates" in {

      val Some(person) = personEntities(withGitLabId, withEmail)
        .map(_.copy(maybeAffiliation = personAffiliations.generateSome))
        .generateOne
        .toMaybe[entities.Person.WithGitLabId]

      upload(to = projectsDataset, person)

      findPersons shouldBe Set(
        (person.resourceId.value,
         person.name.value.some,
         person.maybeEmail.map(_.value),
         person.maybeAffiliation.map(_.value),
         person.gitLabId.value.some
        )
      )

      val queries = preparePostDataUpdates(person)

      queries.runAll(on = projectsDataset).unsafeRunSync()

      findPersons shouldBe Set(
        (person.resourceId.value,
         person.name.value.some,
         person.maybeEmail.map(_.value),
         person.maybeAffiliation.map(_.value),
         person.gitLabId.value.some
        )
      )
    }
  }

  private def findPersons: Set[(String, Option[String], Option[String], Option[String], Option[Int])] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "fetch person data",
        Prefixes of schema -> "schema",
        s"""|SELECT ?id ?name ?email ?affiliation ?sameAsId ?gitlabId
            |WHERE {
            |  GRAPH <${GraphClass.Persons.id.show}> {
            |    ?id a schema:Person .
            |    OPTIONAL { ?id schema:name ?name } .
            |    OPTIONAL { ?id schema:email ?email } .
            |    OPTIONAL { ?id schema:affiliation ?affiliation } .
            |    OPTIONAL { ?id schema:sameAs ?sameAsId.
            |               ?sameAsId a schema:URL;
            |                         schema:additionalType '${Person.gitLabSameAsAdditionalType}';
            |                         schema:identifier ?gitlabId.
            |    }
            |  }
            |}
            |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row =>
        (row("id"), row.get("name"), row.get("email"), row.get("affiliation"), row.get("gitlabId").map(_.toInt))
      )
      .toSet

  private implicit class QueryResultsOps(
      records: Set[(String, Option[String], Option[String], Option[String], Option[Int])]
  ) {
    lazy val toIdAndPropertyPairs: Set[(String, String)] =
      records.foldLeft(Set.empty[(String, String)]) {
        case (pairs, (id, maybeName, maybeEmail, maybeAffiliation, maybeGitLabId)) =>
          pairs ++ Set(maybeName, maybeEmail, maybeAffiliation, maybeGitLabId.map(_.toString)).flatten.map(id -> _)
      }
  }
}
