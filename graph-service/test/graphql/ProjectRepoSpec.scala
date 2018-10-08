package graphql

import models.Project
import play.api.libs.json.JsValue
import utils.AsyncBaseSpec
import sangria.macros.derive._
import sangria.schema._
import sangria.execution._
import sangria.marshalling.playJson._
import sangria.macros._

import scala.concurrent.Future

class ProjectRepoSpec extends AsyncBaseSpec {

  // Bootsrap a query schema on projects

  val QueryType: ObjectType[ProjectRepo, Unit] = ObjectType("Query", fields[ProjectRepo, Unit](
    Field("projects", ListType(SchemaTypes.ProjectType),
      description = Some("Returns a list of all available projects."),
      resolve = _.ctx.projects))
  )

  val schema: Schema[ProjectRepo, Unit] = Schema(QueryType)

  "ProjectsRepo" should {
    "work with graphql" in {
      val query = graphql"""
        query Projects {
          projects {
            id
            url
          }
        }
      """

      for {
        result <- Executor.execute(schema, query, new ProjectRepo)
      } yield {
        val projectsVal = (result \ "data" \ "projects").validate[Seq[Project]]
        projectsVal.isSuccess shouldBe true
      }
    }
  }

}
