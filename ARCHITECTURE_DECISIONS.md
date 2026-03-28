# FINSCOPE Architecture Decisions

This document records infrastructure decisions that differ from the original sprint plans.

---

## ADR-001: Kafka Image Selection

**Date:** 2026-03-28  
**Status:** Accepted  
**Sprint:** 1 (Infrastructure)

### Context
The Sprint 1 constraint block specified `bitnami/kafka:3.5`. During implementation, the Kafka service was configured with `confluentinc/cp-kafka:7.5.0`.

### Decision
**Keep confluentinc/cp-kafka:7.5.0** as the production Kafka image.

### Rationale
- More production-realistic deployment patterns
- Better documentation and community support
- Wider adoption in enterprise environments
- Confluent Platform provides consistent tooling

### Consequences
- All Kafka CLI commands use Confluent paths: `/usr/bin/kafka-topics` (not `/opt/bitnami/kafka/bin/kafka-topics.sh`)
- Bootstrap server inside containers: `kafka:9092`
- Bootstrap server from host machine: `localhost:9092`
- Environment variables use `KAFKA_*` prefix (not `KAFKA_CFG_*`)

### Anti-Drift Rule
**Do not switch back to bitnami in any sprint.** This decision is locked.

---

## ADR-002: Zookeeper Image Selection

**Date:** 2026-03-28  
**Status:** Accepted  
**Sprint:** 1 (Infrastructure)

### Context
The Sprint 1 constraint block specified `bitnami/zookeeper:3.8`. The implementation uses the official `zookeeper:3.8` image for consistency with the Confluent Kafka ecosystem.

### Decision
Use official `zookeeper:3.8` image.

### Rationale
- Better compatibility with Confluent Kafka
- Official Apache image with predictable behavior
- Standard configuration paths

---
