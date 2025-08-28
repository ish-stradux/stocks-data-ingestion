#!/bin/bash

# Docker management scripts for Data Ingestion Service

case "$1" in
    "start")
        echo "Starting Data Ingestion Service..."
        docker-compose up -d
        echo "Services started. Check status with: docker-compose ps"
        ;;
    "stop")
        echo "Stopping Data Ingestion Service..."
        docker-compose down
        echo "Services stopped."
        ;;
    "restart")
        echo "Restarting Data Ingestion Service..."
        docker-compose down
        docker-compose up -d
        echo "Services restarted."
        ;;
    "logs")
        echo "Showing logs..."
        docker-compose logs -f
        ;;
    "status")
        echo "Service status:"
        docker-compose ps
        ;;
    "clean")
        echo "Cleaning up (removing volumes)..."
        docker-compose down -v
        docker system prune -f
        echo "Cleanup completed."
        ;;
    "build")
        echo "Building services..."
        docker-compose build --no-cache
        echo "Build completed."
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|logs|status|clean|build}"
        echo ""
        echo "Commands:"
        echo "  start   - Start all services"
        echo "  stop    - Stop all services"
        echo "  restart - Restart all services"
        echo "  logs    - Show logs from all services"
        echo "  status  - Show service status"
        echo "  clean   - Stop services and remove volumes"
        echo "  build   - Rebuild services"
        exit 1
        ;;
esac
