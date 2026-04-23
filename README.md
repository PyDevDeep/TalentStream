# TalentStream 🔍

> Automated AI Job Hunter. Smart, async scraper that discovers, parses (AI), filters, and notifies about new career opportunities in real-time.

![Python](https://img.shields.io/badge/python-3.13-blue.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-15-336791.svg)
![Redis](https://img.shields.io/badge/redis-7-DC382D.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## 🚀 Features

- **Automated Search** — Scheduled job scraping via Serper API with configurable intervals
- **Intelligent Parsing** — LLM-powered extraction (OpenAI GPT-4 with Gemini fallback) of structured job data from raw HTML
- **Smart Deduplication** — Redis-based atomic duplicate detection preventing redundant processing
- **Customizable Filtering** — Keyword, location, and salary-based job filtering engine
- **Real-time Notifications** — Slack integration for instant job alerts
- **Production-Ready** — Async task queue (TaskIQ), database migrations (Alembic), structured logging (Structlog), and error tracking (Sentry)

## 🛠 Tech Stack

### Core
- **Python 3.13** — Runtime with asyncio support
- **FastAPI** — REST API framework for manual triggers and health checks
- **PostgreSQL 15** — Primary data store with JSONB support
- **Redis 7** — Deduplication cache and task broker

### Task Processing
- **[TaskIQ](https://github.com/taskiq-python/taskiq)** — Async task queue with Redis broker
- **[TaskIQ Scheduler](https://github.com/taskiq-python/taskiq)** — Cron-based job scheduling

### Data & Validation
- **SQLAlchemy 2.0** — Async ORM with declarative models
- **Pydantic v2** — Schema validation with Rust core
- **Alembic** — Database migration management

### External Services
- **[Serper API](https://serper.dev/)** — Google search and web page content extraction
- **OpenAI API** — GPT-4 for job data parsing (primary)
- **Google Gemini** — Fallback LLM provider
- **Slack SDK** — Notification delivery

### DevOps & Observability
- **Docker + Docker Compose** — Containerized deployment
- **Structlog** — Structured JSON logging
- **Sentry** — Error tracking and monitoring
- **Pre-commit** — Code quality enforcement (Ruff linting/formatting)

## 📦 Installation

### Prerequisites
- Docker & Docker Compose
- Python 3.13+ (for local development)
- Poetry 1.8+

### Quick Start

1. **Clone the repository**
```bash
git clone https://github.com/PyDevDeep/TalentStream.git
cd TalentStream
```

2. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env with your API keys and settings
```

Required environment variables:
```env
# Database
DATABASE_URL=postgresql+psycopg://user:password@localhost:5432/jobscraper

# Redis
REDIS_URL=redis://localhost:6379/0

# API Keys
SERPER_API_KEY=your_serper_api_key
OPENAI_API_KEY=your_openai_api_key
GEMINI_API_KEY=your_gemini_api_key  # Fallback

# Slack
SLACK_BOT_TOKEN=xoxb-your-slack-bot-token
SLACK_CHANNEL_ID=C01234567890

# Filters
FILTER_KEYWORDS=python,backend,fastapi
FILTER_LOCATION=Remote
FILTER_SALARY_MIN=50000

# Scheduler
SCRAPE_CRON=0 */6 * * *  # Every 6 hours
SCRAPE_QUERY=python backend developer remote

# Sentry (optional)
SENTRY_DSN=your_sentry_dsn
```

3. **Launch with Docker Compose**
```bash
docker compose up -d
```

This starts:
- PostgreSQL (port 5432)
- Redis (port 6379)
- FastAPI app (port 8000)
- TaskIQ worker
- TaskIQ scheduler

4. **Run database migrations**
```bash
docker compose exec app alembic upgrade head
```

## ▶️ Usage

### Manual Scraping Trigger
```bash
curl -X POST http://localhost:8000/trigger-scrape \
  -H "Content-Type: application/json" \
  -d '{"query": "python senior developer remote"}'
```

### Health Check
```bash
curl http://localhost:8000/health
```

### View Logs
```bash
docker compose logs -f app
docker compose logs -f worker
```

## 🏗 Architecture

### Pipeline Overview

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│  Scheduler  │─────▶│ Scrape Task  │─────▶│ Parse Tasks │
│  (cron)     │      │ (Serper API) │      │ (per URL)   │
└─────────────┘      └──────────────┘      └─────────────┘
                                                    │
                                                    ▼
                            ┌───────────────────────────────┐
                            │  Redis Dedup Check            │
                            │  (atomic SET NX EX)           │
                            └───────────────────────────────┘
                                                    │
                                                    ▼
                            ┌───────────────────────────────┐
                            │  Fetch Page Content           │
                            │  (Serper View API)            │
                            └───────────────────────────────┘
                                                    │
                                                    ▼
                            ┌───────────────────────────────┐
                            │  Strip Noise                  │
                            │  (regex patterns, truncate)   │
                            └───────────────────────────────┘
                                                    │
                                                    ▼
                            ┌───────────────────────────────┐
                            │  LLM Parsing                  │
                            │  (OpenAI GPT-4 → Gemini)      │
                            └───────────────────────────────┘
                                                    │
                                                    ▼
                            ┌───────────────────────────────┐
                            │  Filter Engine                │
                            │  (keywords, location, salary) │
                            └───────────────────────────────┘
                                                    │
                                                    ▼
                            ┌───────────────────────────────┐
                            │  Store in PostgreSQL          │
                            │  (upsert with URL dedup)      │
                            └───────────────────────────────┘
                                                    │
                                                    ▼
                            ┌───────────────────────────────┐
                            │  Notify Task                  │
                            │  (Slack message)              │
                            └───────────────────────────────┘
```

### Key Components

**Tasks** (`app/tasks/`)
- `scrape.py` — Search job URLs via Serper, queue parse tasks
- `parse.py` — Full pipeline: dedup → fetch → parse → filter → store
- `notify.py` — Fetch unnotified jobs from DB, send to Slack

**Services** (`app/services/`)
- `dedup.py` — Redis-based duplicate detection with TTL
- `filter.py` — Multi-criteria job filtering (keywords, location, salary)
- `noise_stripper.py` — HTML noise removal (nav, footer, boilerplate)

**Clients** (`app/clients/`)
- `serper.py` — Wrapper for Serper Search and View APIs
- `llm/router.py` — LLM routing with OpenAI primary, Gemini fallback
- `llm/openai_client.py` — GPT-4 structured output extraction
- `llm/gemini_client.py` — Gemini fallback implementation

**Database** (`app/db/`)
- `models/job.py` — SQLAlchemy Job model with JSONB metadata
- `repository.py` — CRUD operations with upsert and notification tracking
- `session.py` — Async database session factory

## 🧪 Testing

```bash
# Install development dependencies
poetry install

# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test suite
pytest tests/unit/
pytest tests/integration/
```

Test structure:
- `tests/unit/` — Isolated component tests (clients, filters, schemas)
- `tests/integration/` — Database, Redis, and API integration tests
- `tests/e2e/` — Full pipeline end-to-end tests

## 🚀 Deployment

### Production Docker Compose

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

Production overrides include:
- Restart policies
- Resource limits
- Log rotation
- Health checks

### CI/CD Pipeline

GitHub Actions workflows (`.github/workflows/`):
- **ci.yml** — Linting, type checking, tests on every PR
- **deploy.yml** — Docker build and deployment on tag push

## 📊 Monitoring

### Metrics to Track
- **Latency**: p95 < 30s per job (Serper + LLM + store)
- **Throughput**: ≥ 100 jobs/hour with 4 workers
- **Dedup Effectiveness**: < 5% duplicates in Slack
- **Uptime**: 99% scheduler availability
- **Error Rate**: < 2% failed tasks per batch

### Log Aggregation
All services output structured JSON logs via Structlog:
```json
{
  "event": "job_stored_successfully",
  "job_id": 123,
  "url": "https://example.com/job",
  "timestamp": "2026-04-23T12:00:00Z"
}
```

### Error Tracking
Sentry integration captures:
- Unhandled exceptions
- LLM parsing failures
- API rate limit errors
- Database connection issues

## 🔧 Configuration

### Filter Rules
Edit in `.env`:
```env
# Comma-separated keywords (OR logic)
FILTER_KEYWORDS=python,fastapi,asyncio,backend

# Location string matching
FILTER_LOCATION=Remote

# Minimum annual salary (USD)
FILTER_SALARY_MIN=80000
```

### Scheduling
Cron expression in `.env`:
```env
# Run every 6 hours at minute 0
SCRAPE_CRON=0 */6 * * *

# Serper search query
SCRAPE_QUERY=senior python developer remote usa
```

### LLM Routing
Priority order:
1. OpenAI GPT-4 (primary, fastest)
2. Google Gemini (fallback on OpenAI failure)

Configure in `app/clients/llm/router.py`

## 🛡 Rate Limits & Cost Optimization

### Serper API
- **Free Tier**: 2500 credits/month
- **Search**: 2 credit per query
- **View**: 2, 6 or 10 credit per URL
- **Recommended**: Paid plan ($50/month for 50K credits) for production

### OpenAI API
- **Model**: GPT-4 Turbo
- **Estimated Cost**: ~$8/month at 7200 calls/month (240 jobs/day)
- **Input**: ~2000 tokens/call × $0.01/1K = $0.02/call
- **Output**: ~500 tokens/call × $0.03/1K = $0.015/call

### Slack API
- **Rate Limit**: 1 message/second per channel
- **Mitigation**: `asyncio.sleep(1)` between messages

## 📄 License

[INSERT LICENSE TYPE]

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Run pre-commit checks (`pre-commit run --all-files`)
4. Commit changes (`git commit -m 'Add amazing feature'`)
5. Push to branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

## 🐛 Troubleshooting

### Common Issues

**Issue**: Tasks not processing
```bash
# Check worker logs
docker compose logs worker

# Verify Redis connection
docker compose exec app python -c "from redis.asyncio import Redis; import asyncio; asyncio.run(Redis.from_url('redis://redis:6379/0').ping())"
```

**Issue**: Database connection errors
```bash
# Check PostgreSQL status
docker compose ps postgres

# Run migrations
docker compose exec app alembic upgrade head
```

**Issue**: Slack notifications not sending
```bash
# Test Slack token
docker compose exec app python test_notify.py
```

**Issue**: LLM parsing failures
- Check API keys in `.env`
- Review logs for rate limit errors
- Verify noise stripper output length (should be < 5000 chars)

## 📞 Support

For issues and questions:
- GitHub Issues: [https://github.com/PyDevDeep/TalentStream/issues](https://github.com/PyDevDeep/TalentStream/issues)
- Repository: [https://github.com/PyDevDeep/TalentStream](https://github.com/PyDevDeep/TalentStream)

---

**Built with ❤️ using Python, FastAPI, and asyncio**
