package graphql

import models.Activity

class ActivityRepo {
  private lazy val Activities = {
    Utils
      .loadJsonData[Activity]( "/site-data/tutorial-zhbikes/activities.json" )
      .get
  }

  def activity( id: String ): Option[Activity] = Activities.find( _.id == id )

  def activities: Seq[Activity] = Activities
}
