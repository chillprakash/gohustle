# GoHustle Platform

A full-stack application for trading and market data analysis.

## Project Structure

The project is now organized into two main directories:

### Backend

The backend is built with Go and includes:

- Data collection and processing (Go + NATS + SQLite)
- Execution tool (Go + Redis)
- API server for frontend communication
- Tick data export functionality with Parquet format support
- Paper trading capabilities

### Frontend

The frontend is built with Vue.js and provides:

- Modern UI for interacting with the GoHustle platform
- Real-time market data visualization
- Trading interface for both real and paper trading
- Position management

## Development

### Backend

```bash
cd backend
go run main.go
```

### Frontend

```bash
cd frontend
npm install
npm run serve
```
