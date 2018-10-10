package graphql

import models.GenerationEdge

class GenerationEdgeRepo {
  private lazy val Generations = {
    Utils
      .loadJsonData[GenerationEdge](
        "/site-data/tutorial-zhbikes/generations.json"
      )
      .get
  }

  def byEntity( id: String ): Option[GenerationEdge] =
    Generations.find( _.entityId == id )

  def byActivity( id: String ): Seq[GenerationEdge] =
    Generations.filter( _.activityId == id )
}
