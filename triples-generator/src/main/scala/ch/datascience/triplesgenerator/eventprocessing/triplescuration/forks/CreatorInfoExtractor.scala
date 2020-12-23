package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import ch.datascience.graph.Schemas.schema
import ch.datascience.graph.model.users.{Email, Name}
import ch.datascience.rdfstore.JsonLDTriples
import io.circe.{Decoder, Json}
import io.renku.jsonld.Property

trait CreatorInfoExtractor {
  def extract(triples: JsonLDTriples): (List[Name], Option[Email])
}

object CreatorInfoExtratorImpl extends CreatorInfoExtractor {
  override def extract(triples: JsonLDTriples): (List[Name], Option[Email]) =
    (for {
      currentCreatorId <- findCreatorId(triples)
      currentCreator   <- findCreator(currentCreatorId, triples)
    } yield {
      val maybeEmail = getValueIfUnique(schema / "email", currentCreator).map(Email(_))
      val maybeNames =
        currentCreator.hcursor
          .get[List[Json]]((schema / "name").toString)
          .toOption
          .map(names =>
            names.map(jsonLDname => jsonLDname.hcursor.get[String]("@value").toOption).collect { case Some(name) =>
              Name(name)
            }
          )
          .getOrElse(Nil)

      (maybeNames, maybeEmail)
    }).getOrElse((Nil, None))

  private def findCreatorId(triples: JsonLDTriples) = triples.value.findAllByKey((schema / "creator").toString) match {
    case Nil    => None
    case x :: _ => x.hcursor.get[String]("@id").toOption
  }

  private def findCreator(currentCreatorId: String, triples: JsonLDTriples): Option[Json] =
    triples.value.asArray.flatMap(vector =>
      vector
        .find { json =>
          json.hcursor.get[String]("@id").contains(currentCreatorId) && json.hcursor
            .get[List[String]]("@type")
            .map(_.contains((schema / "Person").toString))
            .getOrElse(false)
        }
    )

  private def getValueIfUnique(property: Property, json: Json): Option[String] =
    json.hcursor
      .downField(property.toString)
      .as[List[Json]]
      .map {
        case x :: Nil => x.hcursor.get[String]("@value").toOption
        case _        => None
      }
      .getOrElse(None)
}
