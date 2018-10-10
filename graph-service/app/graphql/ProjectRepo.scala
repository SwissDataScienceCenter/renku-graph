package graphql

import models.Project

class ProjectRepo {
  private lazy val Projects = {
    Utils
      .loadJsonData[Project]( "/site-data/tutorial-zhbikes/projects.json" )
      .get
  }

  def project( id: String ): Option[Project] = Projects.find( _.id == id )

  def projects: Seq[Project] = Projects
}
