package graphql

import persistence.DatabaseAccessLayer
import play.api.Logger
import play.api.libs.json.JsValue
import sangria.execution._
import sangria.macros._
import sangria.marshalling.playJson._
import sangria.renderer.SchemaRenderer
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import utils.AsyncBaseSpec

import scala.concurrent.Future

class QuerySpec extends AsyncBaseSpec {

  lazy val logger: Logger = Logger( "application.graphql.Queryspec" )

  // Database setup
  val dbConfig = DatabaseConfig.forConfig[JdbcProfile]( "slick.dbs.default" )
  val dal = DatabaseAccessLayer( dbConfig )

  "sangria" should {
    "render the query schema" in {
      val rendered = SchemaRenderer.renderSchema( graphql.schema )
      logger.info( s"GraphQL schema:\n$rendered" )
      true shouldBe true
    }

    "resolve a graphql query" in {
      val query = gql"""
        query Entities {
          entities {
            path
            commit_sha1
            wasGeneratedBy {
              id
              label
              used {
                path
                commit_sha1
              }
              qualifiedAssociationPlan {
                path
              }
            }
            invUsed {
              label
            }
          }
        }
      """

      for {
        _ <- dal.init
        json <- Executor.execute(
          graphql.schema,
          query,
          userContext      = UserContext( dal ),
          deferredResolver = graphql.resolver
        ): Future[JsValue]
      } yield {
        logger.info( s"graphql result: $json" )
        val entities = ( json \ "data" \ "entities" ).as[Seq[JsValue]]
        entities should not be empty
      }
    }
  }

}
