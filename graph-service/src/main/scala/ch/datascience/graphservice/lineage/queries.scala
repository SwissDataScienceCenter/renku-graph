package ch.datascience.graphservice.lineage

import ch.datascience.graph.model.events.ProjectPath
import sangria.ast
import sangria.schema._
import sangria.validation.ValueCoercionViolation

private object queries {

  import schema._

  private implicit val ProjectPathType: ScalarType[ProjectPath] = {
    import cats.implicits._
    case object ProjectPathCoercionViolation
        extends ValueCoercionViolation("ProjectPath value expected in format <namespace>/<project>")

    ScalarType[ProjectPath](
      name         = "ProjectPath",
      description  = Some("Project's path in the GitLab."),
      coerceOutput = valueOutput,
      coerceUserInput = {
        case s: String => ProjectPath.from(s) leftMap (_ => ProjectPathCoercionViolation)
        case _ => Left(ProjectPathCoercionViolation)
      },
      coerceInput = {
        case ast.StringValue(s, _, _, _, _) => ProjectPath.from(s) leftMap (_ => ProjectPathCoercionViolation)
        case _                              => Left(ProjectPathCoercionViolation)
      }
    )
  }

  val ProjectPathArgument = Argument("projectPath", ProjectPathType)

  val QueryType = ObjectType(
    name = "Query",
    fields = fields[IOLineageRepository, Unit](
      Field(
        name        = "lineage",
        fieldType   = OptionType(LineageType),
        description = Some("Returns a lineage for a project with the given path"),
        arguments   = List(ProjectPathArgument),
        resolve     = context => context.ctx.findLineage(context.args arg ProjectPathArgument).unsafeToFuture()
      )
    )
  )

  val lineageQuerySchema: Schema[IOLineageRepository, Unit] = Schema(QueryType)
}
