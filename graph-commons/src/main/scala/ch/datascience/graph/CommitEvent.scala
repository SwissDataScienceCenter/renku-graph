package ch.datascience.graph

import java.time.Instant

case class CommitEvent(
    sha1:      Array[Byte],
    message:   String,
    timestamp: Instant,
    pushUser:  User,
    author:    User,
    committer: User,
    parents:   Iterable[Array[Byte]]
)
