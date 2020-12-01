package ch.datascience.triplesgenerator.eventprocessing.triplescuration
package persondetails

import cats.MonadError
import cats.data.NonEmptyList
import cats.syntax.all._
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsUpdater.Person
import io.circe.Json
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import monocle.function.Plated

import scala.collection.mutable

private class PersonExtractor[Interpretation[_]]()(implicit ME: MonadError[Interpretation, Throwable]) {

  def extractPersons(triples: JsonLDTriples): Interpretation[(JsonLDTriples, Set[Person])] = {
    val persons = mutable.HashSet[Person]()
    for {
      updatedJson <- Plated.transformM(toJsonWithoutPersonDetails(persons))(triples.value)
    } yield JsonLDTriples(updatedJson) -> persons.toSet
  }

  private def toJsonWithoutPersonDetails(persons: mutable.Set[Person])(json: Json): Interpretation[Json] =
    root.`@type`.each.string.getAll(json) match {
      case types if types.contains("http://schema.org/Person") =>
        findPersonData(json) match {
          case None => json.pure[Interpretation]
          case Some(personData) =>
            for {
              foundPerson <- personData.toPerson
              _ = persons add foundPerson
            } yield removeNameAndEmail(json)
        }
      case _ => json.pure[Interpretation]
    }

  private def findPersonData(json: Json) =
    for {
      entityId <- json.get[ResourceId]("@id") flatMap skipBlankNodes
      personNames  = json.getValues[Name]("http://schema.org/name")
      personEmails = json.getValues[Email]("http://schema.org/email")
    } yield (entityId, personNames, personEmails)

  private implicit class PersonDataOps(personData: (ResourceId, List[Name], List[Email])) {

    private val (entityId, personNames, personEmails) = personData

    lazy val toPerson: Interpretation[Person] = for {
      nonEmptyNames <- toNonEmptyList(personNames)
    } yield Person(entityId, nonEmptyNames, personEmails.toSet)

    private lazy val toNonEmptyList: List[Name] => Interpretation[NonEmptyList[Name]] = {
      case Nil =>
        ME.raiseError {
          new Exception(s"No names for person with '$entityId' id found in generated JSON-LD")
        }
      case first :: other =>
        NonEmptyList.of(first, other: _*).pure[Interpretation]
    }
  }

  private def removeNameAndEmail(json: Json) =
    json
      .remove("http://schema.org/name")
      .remove("http://schema.org/email")
      .remove("http://www.w3.org/2000/01/rdf-schema#label")

  private lazy val skipBlankNodes: ResourceId => Option[ResourceId] = id =>
    if (id.value startsWith "mailto:") Some(id)
    else None
}
