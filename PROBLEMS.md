# Problem

## Split votes
When the leader node crashes, many of the follower nodes become candidates but
none of them can get the majority votes.

### Solutions
- Our algorithm: delay response and choose the candidate with largest $T$.
- Maybe we can have a larger `election_timeout`?
- There is a bug in raftos code. When `self.storage.term < data['term']`, `self.storage.update` only updates `term` but not `voted_for`, so that the candidate in the next term cannot get the majority votes. To fix this, update `voted_for` to `None`.
