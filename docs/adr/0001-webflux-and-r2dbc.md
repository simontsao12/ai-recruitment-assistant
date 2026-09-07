# ADR 0001: WebFlux and R2DBC

## Status

Accepted.

## Context

The application uses Spring WebFlux. JDBC calls, synchronous model calls and document parsing can block their calling thread, so their execution needs to be separated from Netty event-loop threads.

## Decision

Use Spring Data R2DBC for business data access and JDBC during startup for Liquibase migrations through `spring-boot-starter-liquibase`.

Use `TransactionalOperator` to group related database writes, including business records, processed-event markers and Outbox events. Execute model calls and external HTTP requests outside these database transactions.

Run synchronous model calls and file storage/parsing operations on `Schedulers.boundedElastic()`.

Spring Kafka listeners run on dedicated listener threads. They explicitly call `block(Duration)` to wait for reactive processing before returning. This blocking bridge belongs to the Kafka listener boundary, not the WebFlux request path.

## Consequences

- Business database access uses reactive APIs.
- Transaction boundaries explicitly follow the reactive chain.
- Database transactions do not cover external API calls or filesystem writes.
- Agent and extraction/review listener timeouts default to 10 and 12 minutes; Kafka max.poll.interval.ms defaults to 15 minutes. Startup validation checks their ordering and margin. See the README timeout settings for configuration.
- Moving work to `boundedElastic()` isolates blocking operations; it does not make the underlying calls non-blocking.


A reactive timeout ends the wait but does not guarantee that an in-flight synchronous model call has stopped. Recovery must account for work that may still be running.
