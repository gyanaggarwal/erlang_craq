# erlang_craq
Chain Replication with Apportioned Queries High Throughput Atomic Store
=======================================================================

Erlang implementation erlang_craq combines the ideas given in floowing 2 papers

1.  "A High Throughput Atomic Storage Algorithm"
by Rachid Guerraoui, Dejan Kostic, Ron R. Levy and Vivien Quema.

2. "Object Storage on CRAQ : High-throughput chain replication for read-mostly workloads"
by Jeff Terrace and Michael J. Freeman.

We remove the restriction that CRAQ can have only one head and allows clients to write to any
node concurrently thus achieving a high write throughput.

It prevents concurrent updates on the same object to maintain consistency.

We also implemented mini-transactions where client can update multiple objects in a single
invocation.

It is an atomic, high throughput, resilient distributed store. It will always be consistent and
available despite of server failures (CA).

It assumes that a point-to-point communication is always available between all the servers
(partition intolerant). A failure detection mechanism is setup to detect any server failure
and system will continue to work as long as at least one server is available.

It solves the read-inversion problem and prevents any read from returning an old value after
an earlier read returned a new value.

Please refer to accompanying document erlang_craq.pdf for more details.


