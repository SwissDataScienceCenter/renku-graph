package graphql

import models.Entity

class EntityRepo {
  private lazy val Entities = {
    Utils
      .loadJsonData[Entity]( "/site-data/tutorial-zhbikes/entities.json" )
      .get
  }

  def entity( id: String ): Option[Entity] = Entities.find( _.id == id )

  def entities: Seq[Entity] = Entities
}
