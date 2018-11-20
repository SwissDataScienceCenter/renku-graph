package controllers

import graphql.{ UserContext, UserContextProvider }
import javax.inject.Inject
import play.api.libs.json._
import play.api.mvc._
import sangria.execution.{ ExceptionHandler, Executor, HandledException, _ }
import sangria.marshalling.playJson._
import sangria.parser.{ QueryParser, SyntaxError }
import sangria.renderer.SchemaRenderer

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

class ApplicationController @Inject() (
    userContextProvider: UserContextProvider,
    cc:                  ControllerComponents
)
  extends AbstractController( cc ) {
  implicit lazy val ec: ExecutionContext = cc.executionContext

  lazy val NoContentAction: ActionBuilder[Request, AnyContent] = Action( cc.parsers.anyContent( Some( 0L ) ) )

  def graphqlEndpoint( query: String, variables: Option[String], operation: Option[String] ): Action[AnyContent] = NoContentAction.async {
    implicit request =>
      executeQuery( query, variables.map( parseVariables ), operation )
  }

  def graphqlEndpointPost: Action[JsValue] = Action.async( cc.parsers.tolerantJson ) { implicit request =>
    val query = ( request.body \ "query" ).as[String]
    val operation = ( request.body \ "operationName" ).asOpt[String]
    val variables = ( request.body \ "variables" ).toOption.flatMap {
      case JsString( vars ) => Some( parseVariables( vars ) )
      case obj: JsObject    => Some( obj )
      case _                => None
    }

    executeQuery( query, variables, operation )
  }

  def schema: Action[AnyContent] = NoContentAction { implicit request =>
    Ok( SchemaRenderer.renderSchema( graphql.schema ) )
  }

  def ping: Action[AnyContent] = NoContentAction { implicit request =>
    Ok( "pong" )
  }

  private def parseVariables( variables: String ): JsObject = {
    if ( variables.trim == "" || variables.trim == "null" ) Json.obj()
    else Json.parse( variables ).as[JsObject]
  }

  private def executeQuery( query: String, variables: Option[JsObject], operation: Option[String] ): Future[Result] = {
    QueryParser.parse( query ) match {

      // query parsed successfully, time to execute it!
      case Success( queryAst ) =>
        val userContext = userContextProvider.get

        Executor.execute(
          graphql.schema,
          queryAst,
          userContext      = userContext,
          deferredResolver = graphql.resolver,
          exceptionHandler = exceptionHandler,
          queryReducers    = List(
            QueryReducer.rejectMaxDepth[UserContext]( 15 ),
            QueryReducer.rejectComplexQueries[UserContext](
              4000,
              ( _, _ ) => TooComplexQueryError
            )
          )
        )
          .map( Ok( _ ) )
          .recover {
            case error: QueryAnalysisError => BadRequest( error.resolveError )
            case error: ErrorWithResolver =>
              InternalServerError( error.resolveError )
          }

      // can't parse GraphQL query, return error
      case Failure( error: SyntaxError ) =>
        Future.successful(
          BadRequest(
            Json.obj(
              "syntaxError" -> error.getMessage,
              "locations" -> Json.arr(
                Json.obj(
                  "line" -> error.originalError.position.line,
                  "column" -> error.originalError.position.column
                )
              )
            )
          )
        )

      case Failure( error ) =>
        throw error
    }
  }

  private lazy val exceptionHandler = ExceptionHandler {
    case ( _, error@TooComplexQueryError )           => HandledException( error.getMessage )
    case ( _, error@MaxQueryDepthReachedError( _ ) ) => HandledException( error.getMessage )
  }

  private case object TooComplexQueryError extends Exception( "Query is too expensive." )

}
