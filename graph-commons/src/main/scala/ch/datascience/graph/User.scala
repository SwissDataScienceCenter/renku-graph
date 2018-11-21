package ch.datascience.graph

case class User(
    username: String,
    email:    String,
    gitlabId: Option[Int]
)
