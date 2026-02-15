# LedgerQ Examples

Learn LedgerQ through runnable examples. Start with **simple** and progress through features.

## Learning Path

### 🔰 Getting Started
1. **[simple](simple/)** - Basic enqueue/dequeue operations ⭐ **Start here!**
2. **[producer-consumer](producer-consumer/)** - Concurrent access patterns

### 🚀 Core Features
3. **[ttl](ttl/)** - Message expiration with time-to-live
4. **[headers](headers/)** - Message metadata and key-value pairs
5. **[replay](replay/)** - Seeking and time-travel through message history
6. **[streaming](streaming/)** - Real-time event processing with push-based API

### ⚡ Advanced Features (v1.1.0+)
7. **[priority](priority/)** - Priority ordering (v1.1.0)
8. **[dlq](dlq/)** - Dead Letter Queue and retry handling (v1.2.0)
9. **[compression](compression/)** - Payload compression with GZIP (v1.3.0)
10. **[deduplication](deduplication/)** - Idempotent message processing (v1.4.0)

### 📊 Operational
11. **[metrics](metrics/)** - Monitoring and observability

## Running Examples

All examples are self-contained with clear output:

```bash
cd examples/simple
go run main.go
```

## Quick Reference

| Example | Feature | Use Case |
|---------|---------|----------|
| simple | Basic ops | First-time users |
| producer-consumer | Concurrency | Multi-threaded processing |
| ttl | Expiration | Temporary messages, caches |
| headers | Metadata | Message tagging, routing |
| replay | Seeking | Reprocessing, debugging |
| streaming | Real-time | Event-driven architectures |
| priority | Ordering | Urgent task processing |
| dlq | Failures | Error handling, retries |
| compression | Disk saving | Large payloads (JSON, logs) |
| deduplication | Idempotency | Exactly-once semantics |
| metrics | Monitoring | Production observability |

## Example Structure

Each example demonstrates:
- **Setup**: How to configure the queue
- **Core API**: Key operations for that feature
- **Output**: What you'll see when running
- **Use cases**: When to use this pattern

## Need Help?

- **Documentation**: See [docs/USAGE.md](../docs/USAGE.md) for complete API reference
- **Issues**: Report problems at https://github.com/1mb-dev/ledgerq/issues
- **Questions**: Check existing issues or open a discussion

## Contributing Examples

Found a useful pattern? Consider contributing an example:
1. Create directory: `examples/your-feature/`
2. Add `main.go` with clear comments
3. Add `README.md` following our template (see `streaming/README.md`)
4. Update this index
5. Submit PR

---

**Total Examples**: 11 | **Difficulty**: 🟢 Beginner → 🟡 Intermediate → 🔴 Advanced
