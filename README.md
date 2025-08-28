# Data Ingestion Service

A FastAPI service that downloads parquet files from HuggingFace and loads them into PostgreSQL using chunked processing and parallel workers.

## Features

- Downloads parquet files from HuggingFace datasets
- Chunked processing for large files
- Parallel processing with configurable workers
- Progress tracking
- PostgreSQL integration
- Health checks
- Docker support

## Quick Start with Docker

### Prerequisites
- Docker
- Docker Compose

### Running the Service

1. **Start the services:**
   ```bash
   docker-compose up -d
   ```

2. **Check service status:**
   ```bash
   docker-compose ps
   ```

3. **View logs:**
   ```bash
   # View all logs
   docker-compose logs -f
   
   # View specific service logs
   docker-compose logs -f data-ingestion
   docker-compose logs -f postgres
   ```

4. **Access the API:**
   - API Documentation: http://localhost:8000/docs
   - Health Check: http://localhost:8000/health
   - Progress: http://localhost:8000/progress

### API Endpoints

- `GET /load_parquet` - Start the data ingestion process
- `GET /progress` - Get current processing progress
- `GET /health` - Health check
- `GET /load_parquet_config` - Get processing configuration

### Example Usage

1. **Start data ingestion:**
   ```bash
   curl http://localhost:8000/load_parquet
   ```

2. **Check progress:**
   ```bash
   curl http://localhost:8000/progress
   ```

3. **Health check:**
   ```bash
   curl http://localhost:8000/health
   ```

### Stopping the Services

```bash
docker-compose down
```

To also remove volumes (database data):
```bash
docker-compose down -v
```

## Configuration

The service uses the following environment variables:

- `DATABASE_URL`: PostgreSQL connection string (default: postgresql+psycopg2://postgres:1234@postgres:5432/yahoo)

## Development

### Running Locally (without Docker)

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start PostgreSQL locally:**
   ```bash
   # Using Docker for just the database
   docker run -d --name postgres -e POSTGRES_DB=yahoo -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=1234 -p 5432:5432 postgres:15-alpine
   ```

3. **Run the application:**
   ```bash
   python main.py
   ```

## Architecture

- **FastAPI**: Web framework
- **PostgreSQL**: Database for storing parquet data
- **Pandas**: Data processing
- **SQLAlchemy**: Database ORM
- **ThreadPoolExecutor**: Parallel processing
- **Chunked processing**: Memory-efficient handling of large files

## Performance Features

- Chunked file downloads with progress tracking
- Parallel processing with configurable worker threads
- Memory-efficient parquet processing
- Database connection pooling
- Progress monitoring and logging
