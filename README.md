# GoHustle

GoHustle is a sophisticated trading automation system built in Go, designed to interact with the Zerodha Kite Connect API for automated trading operations.

## Features

- Real-time market data streaming using Zerodha's KiteConnect API
- Automated instrument data management and synchronization
- Support for both spot indices and derivatives trading
- Built-in scheduler for automated trading operations
- Integrated logging system
- PostgreSQL database integration for data persistence
- Telegram notifications support
- Redis caching for improved performance
- Docker support for easy deployment

## Prerequisites

- Go 1.23 or higher
- PostgreSQL database
- Redis server
- Zerodha trading account with Kite Connect API access
- Telegram Bot (for notifications)

## Configuration

The application uses environment variables for configuration. Create a `.env` file in the root directory with the following variables:

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
POSTGRES_DB=hustledb
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   go mod download
   ```

## Running the Application

### Using Docker

```bash
docker-compose up -d
```

### Manual Run

```bash
go run main.go
```

## Project Structure

- `/cache` - Caching implementation
- `/config` - Configuration management
- `/db` - Database related code
- `/logger` - Logging implementation
- `/notify` - Notification system (Telegram)
- `/scheduler` - Trading scheduler implementation
- `/zerodha` - Zerodha API integration
- `/tools` - Utility functions and tools
- `/k8s` - Kubernetes deployment configurations

## Features in Detail

### Market Data Handling
- Real-time market data streaming
- Instrument data synchronization
- Expiry management for derivatives

### Trading Operations
- Automated trading execution
- Support for multiple indices
- Token management for different instruments

### Infrastructure
- Containerized deployment support
- Kubernetes configurations
- Logging and monitoring

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is proprietary software. All rights reserved.

## Support

For support, please contact the development team.
