package graphql

import java.io.InputStream

import models.Project
import play.api.libs.json.{JsResult, JsSuccess, Json, Reads}

import scala.util.Try

class ProjectRepo {
  private lazy val Projects = {
    ProjectRepo
      .loadJsonData[Project]("/site-data/tutorial-zhbikes/projects.json")
      .get
  }

  def project(id: String): Option[Project] = Projects.find(_.id == id)

  def projects: Seq[Project] = Projects
}

object ProjectRepo {

  def loadJsonData[T: Reads](is: InputStream): Try[Seq[T]] = {
    val json = Json.parse(is)
    JsResult.toTry(json.validate[Seq[T]])
  }

  def loadJsonData[T: Reads](path: String): Try[Seq[T]] = {
    val is = getClass.getResourceAsStream(path)
    loadJsonData(is)
  }

}
