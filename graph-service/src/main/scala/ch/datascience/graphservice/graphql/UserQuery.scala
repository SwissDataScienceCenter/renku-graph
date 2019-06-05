package ch.datascience.graphservice.graphql

import sangria.ast.Document

final case class UserQuery(query: Document, operation: Option[String])
