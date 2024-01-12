/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.persons

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.entities.Person
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{GraphClass, entities}
import io.renku.http.client.UrlEncoder
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.persons.UpdatesCreatorSpec.PersonData
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQuery}
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class UpdatesCreatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with EntitiesGenerators
    with should.Matchers {

  import UpdatesCreator._

  "preparePreDataUpdates" should {

    "generate queries which delete person's name, email and affiliation " +
      "in case all of them were changed" in projectsDSConfig.use { implicit pcc =>
        val Some(kgPerson) = personEntities(withGitLabId, withEmail)
          .map(_.copy(maybeAffiliation = personAffiliations.generateSome))
          .generateOne
          .toMaybe[entities.Person.WithGitLabId]
        val mergedPerson = kgPerson.copy(name = personNames.generateOne,
                                         maybeEmail = personEmails.generateSome,
                                         maybeAffiliation = personAffiliations.generateSome
        )

        for {
          _ <- uploadToProjects(kgPerson)

          _ <- findPersons.asserting(
                 _ shouldBe Set(
                   PersonData(
                     kgPerson.resourceId.value,
                     kgPerson.name.value.some,
                     kgPerson.maybeEmail.map(_.value),
                     kgPerson.maybeAffiliation.map(_.value),
                     kgPerson.gitLabId.value.some
                   )
                 )
               )

          _ <- runUpdates(preparePreDataUpdates(kgPerson, mergedPerson))

          _ <- findPersons.asserting(
                 _ shouldBe Set(PersonData(kgPerson.resourceId.value, None, None, None, kgPerson.gitLabId.value.some))
               )
        } yield Succeeded
      }

    "generate queries which delete person's name, email and affiliation " +
      "in case they are removed" in projectsDSConfig.use { implicit pcc =>
        val Some(kgPerson) = personEntities(withGitLabId, withEmail)
          .map(_.copy(maybeAffiliation = personAffiliations.generateSome))
          .generateOne
          .toMaybe[entities.Person.WithGitLabId]
        val mergedPerson = kgPerson.copy(maybeEmail = None, maybeAffiliation = None)

        for {
          _ <- uploadToProjects(kgPerson)

          _ <- findPersons.asserting(
                 _ shouldBe Set(
                   PersonData(
                     kgPerson.resourceId.value,
                     kgPerson.name.value.some,
                     kgPerson.maybeEmail.map(_.value),
                     kgPerson.maybeAffiliation.map(_.value),
                     kgPerson.gitLabId.value.some
                   )
                 )
               )

          _ <- runUpdates(preparePreDataUpdates(kgPerson, mergedPerson))

          _ <- findPersons.asserting(
                 _ shouldBe Set(
                   PersonData(kgPerson.resourceId.value,
                              Some(kgPerson.name.value),
                              None,
                              None,
                              kgPerson.gitLabId.value.some
                   )
                 )
               )
        } yield Succeeded
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

    "generate queries which delete person's duplicate name, email and/or affiliation" in projectsDSConfig.use {
      implicit pcc =>
        val Some(person) = personEntities(withGitLabId, withEmail)
          .map(_.copy(maybeAffiliation = personAffiliations.generateSome))
          .generateOne
          .toMaybe[entities.Person.WithGitLabId]
        val duplicatePerson = person.copy(name = personNames.generateOne,
                                          maybeEmail = personEmails.generateSome,
                                          maybeAffiliation = personAffiliations.generateSome
        )

        for {
          _ <- uploadToProjects(person, duplicatePerson)

          _ <- findPersons.asserting(
                 _.toIdAndPropertyPairs shouldBe Set(
                   (person.resourceId.value -> person.name.value).some,
                   person.maybeEmail.map(person.resourceId.value -> _.value),
                   person.maybeAffiliation.map(person.resourceId.value -> _.value),
                   (person.resourceId.value          -> person.gitLabId.value.toString).some,
                   (duplicatePerson.resourceId.value -> duplicatePerson.name.value).some,
                   duplicatePerson.maybeEmail.map(duplicatePerson.resourceId.value -> _.value),
                   duplicatePerson.maybeAffiliation.map(duplicatePerson.resourceId.value -> _.value),
                   (duplicatePerson.resourceId.value -> duplicatePerson.gitLabId.value.toString).some
                 ).flatten
               )

          _ <- runUpdates(preparePostDataUpdates(person))

          givenPersons = List(person, duplicatePerson)
          persons <- findPersons
          _ = persons.size shouldBe 1

          // TODO the givenPersons.name is url-encoded whereas persons.head.name is not (?)
          _ = persons.head.name
                .map(UrlEncoder.urlEncode) should contain oneElementOf givenPersons.flatMap(_.name).map(_.value)

          _ = persons.head.email       should contain oneElementOf givenPersons.flatMap(_.maybeEmail).map(_.value)
          _ = persons.head.affiliation should contain oneElementOf givenPersons.flatMap(_.maybeAffiliation).map(_.value)
        } yield Succeeded
    }

    "generate queries which do nothing if there are no duplicates" in projectsDSConfig.use { implicit pcc =>
      val Some(person) = personEntities(withGitLabId, withEmail)
        .map(_.copy(maybeAffiliation = personAffiliations.generateSome))
        .generateOne
        .toMaybe[entities.Person.WithGitLabId]

      for {
        _ <- uploadToProjects(person)

        _ <- findPersons.asserting(
               _ shouldBe Set(
                 PersonData(person.resourceId.value,
                            person.name.value.some,
                            person.maybeEmail.map(_.value),
                            person.maybeAffiliation.map(_.value),
                            person.gitLabId.value.some
                 )
               )
             )

        _ <- runUpdates(preparePostDataUpdates(person))

        _ <- findPersons.asserting(
               _ shouldBe Set(
                 PersonData(person.resourceId.value,
                            person.name.value.some,
                            person.maybeEmail.map(_.value),
                            person.maybeAffiliation.map(_.value),
                            person.gitLabId.value.some
                 )
               )
             )
      } yield Succeeded
    }
  }

  private def findPersons(implicit pcc: ProjectsConnectionConfig): IO[Set[PersonData]] =
    runSelect(
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
    ).map(
      _.map(row =>
        PersonData(row("id"),
                   row.get("name"),
                   row.get("email"),
                   row.get("affiliation"),
                   row.get("gitlabId").map(_.toInt)
        )
      ).toSet
    )

  private implicit class QueryResultsOps(
      records: Set[PersonData]
  ) {
    lazy val toIdAndPropertyPairs: Set[(String, String)] =
      records.foldLeft(Set.empty[(String, String)]) {
        case (pairs, PersonData(id, maybeName, maybeEmail, maybeAffiliation, maybeGitLabId)) =>
          pairs ++ Set(maybeName, maybeEmail, maybeAffiliation, maybeGitLabId.map(_.toString)).flatten.map(id -> _)
      }
  }

  private implicit lazy val ioLogger: TestLogger[IO] = TestLogger()
}

object UpdatesCreatorSpec {
  final case class PersonData(
      id:          String,
      name:        Option[String],
      email:       Option[String],
      affiliation: Option[String],
      gitlabId:    Option[Int]
  )
}
