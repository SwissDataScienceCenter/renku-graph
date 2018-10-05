package models
import java.time.Instant

case class Activity(
    id: String,
    label: String,
    commit_sha1: String,
    endTime: Instant,
)
