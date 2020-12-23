package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import ch.datascience.graph.Schemas.schema
import ch.datascience.graph.model.users.{Email, Name}
import ch.datascience.rdfstore.JsonLDTriples
import io.circe.Json

trait CreatorInfoExtractor {
  def extract(triples: JsonLDTriples): (Option[Name], Option[Email])
}

object CreatorInfoExtratorImpl extends CreatorInfoExtractor {
  override def extract(triples: JsonLDTriples): (Option[Name], Option[Email]) =
    (for {
      currentCreatorId <- findCreatorId(triples)
      currentCreator   <- findCreator(currentCreatorId, triples)
    } yield {
      val maybeEmail =
        currentCreator.hcursor.downField((schema / "email").toString).get[String]("@value").toOption.map(Email(_))
      val maybeName =
        currentCreator.hcursor.downField((schema / "name").toString).get[String]("@value").toOption.map(Name(_))
      (maybeName, maybeEmail)
    }).getOrElse((None, None))

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

}
