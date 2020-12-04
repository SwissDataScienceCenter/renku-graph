package ch.datascience.triplesgenerator.eventprocessing.triplescuration
package persondetails

import cats.MonadError
import cats.syntax.all._
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.Json
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import monocle.function.Plated

import scala.collection.mutable

private trait PersonExtractor[Interpretation[_]] {
  def extractPersons(triples: JsonLDTriples): Interpretation[(JsonLDTriples, Set[Person])]
}

private class PersonExtractorImpl[Interpretation[_]]()(implicit ME: MonadError[Interpretation, Throwable])
    extends PersonExtractor[Interpretation] {

  override def extractPersons(triples: JsonLDTriples): Interpretation[(JsonLDTriples, Set[Person])] = {
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
    json
      .get[ResourceId]("@id")
      .flatMap { entityId =>
        (json.getValues[Name]("http://schema.org/name"), json.getValues[Email]("http://schema.org/email")) match {
          case (Nil, Nil)                  => None
          case (personNames, personEmails) => Some(entityId, personNames, personEmails)
        }
      }

  private implicit class PersonDataOps(personData: (ResourceId, List[Name], List[Email])) {

    private val (entityId, personNames, personEmails) = personData

    lazy val toPerson: Interpretation[Person] = for {
      name       <- toSingleName(personNames)
      maybeEmail <- toSingleEmail(personEmails)

    } yield Person(entityId, None, name, maybeEmail)

    private lazy val toSingleName: List[Name] => Interpretation[Name] = {
      case Nil =>
        ME.raiseError {
          new Exception(s"No names for person with '$entityId' id found in generated JSON-LD")
        }
      case first :: Nil =>
        first.pure[Interpretation]
      case _ =>
        ME.raiseError {
          new Exception(s"Multiple names for person with '$entityId' id found in generated JSON-LD")
        }

    }

    private lazy val toSingleEmail: List[Email] => Interpretation[Option[Email]] = {
      case Nil          => Option.empty[Email].pure[Interpretation]
      case first :: Nil => first.some.pure[Interpretation]
      case _ =>
        ME.raiseError {
          new Exception(s"Multiple emails for person with '$entityId' id found in generated JSON-LD")
        }

    }
  }

  private def removeNameAndEmail(json: Json) =
    json
      .remove("http://schema.org/name")
      .remove("http://schema.org/email")
      .remove("http://www.w3.org/2000/01/rdf-schema#label")

}

private object PersonExtractor {
  def apply[Interpretation[_]]()(implicit
      ME: MonadError[Interpretation, Throwable]
  ): PersonExtractor[Interpretation] =
    new PersonExtractorImpl[Interpretation]()
}
