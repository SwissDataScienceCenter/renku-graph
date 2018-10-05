package models

case class Entity(
    id: String,
    path: String,
    commit_sha1: String,
    isPlan: Boolean,
)
