# GoHustle API Documentation

This document describes the REST API endpoints available in the GoHustle trading platform.

## Authentication

Most API endpoints require authentication using a Kite Connect API token.

**Headers:**
- `X-Kite-Token`: Your Kite Connect API token
- `X-User-ID`: (Optional) User identifier for multi-user setups

## Instrument Identification

GoHustle supports two methods for identifying instruments in order placement:

1. **Using Instrument Token** (Recommended): Provide just the `instrumentToken` parameter, and GoHustle will automatically resolve the trading symbol and exchange.

2. **Using Trading Symbol and Exchange**: Provide both the `tradingSymbol` and `exchange` parameters explicitly.

Instrument tokens can be obtained from the option chain response or other market data endpoints.

## Order Management

### Place Order

Places a new order with Zerodha or as a paper trade.

**Endpoint:** `POST /api/orders/place`

**Request:**
```json
{
  "instrumentToken": "12345",  
  "orderType": "MARKET",
  "side": "BUY",
  "quantity": 75,
  "price": 0,
  "triggerPrice": 0,
  "product": "NRML",
  "validity": "DAY",
  "disclosedQty": 0,
  "tag": "GoHustle",
  "paperTrading": false
}
```

**Response:**
```json
{
  "order_id": "230430000012345",
  "status": "success",
  "message": "Order placed successfully",
  "paper_trading": false
}
```

**Parameters:**

**Instrument Identification (choose one method):**
- `instrumentToken`: Instrument token (recommended) - uniquely identifies the instrument without needing symbol and exchange
  
  OR
  
- `tradingSymbol`: Trading symbol of the instrument
- `exchange`: Exchange code (NSE, BSE, NFO, etc.)

**Order Parameters (required):**
- `orderType`: Type of order (MARKET, LIMIT, SL, SL-M)
- `side`: Order side (BUY, SELL)
- `quantity`: Order quantity
- `product`: Product type (NRML, MIS, CNC)

**Conditional Parameters:**
- `price`: Order price (required for LIMIT orders)
- `triggerPrice`: Trigger price (required for SL and SL-M orders)

**Optional Parameters:**
- `validity`: Order validity (DAY, IOC) - defaults to DAY if not specified
- `disclosedQty`: Disclosed quantity
- `tag`: Custom tag for the order
- `paperTrading`: If true, order will be simulated and not sent to broker

### Place GTT Order

Places a Good Till Triggered (GTT) order that executes when specified price conditions are met.

**Endpoint:** `POST /api/orders/gtt`

**Request:**
```json
{
  "triggerType": "single",
  "instrumentToken": "12345",
  "triggerValues": [19500],
  "lastPrice": 19450,
  "orders": [
    {
      "transaction_type": "BUY",
      "quantity": 75,
      "price": 19500,
      "order_type": "LIMIT",
      "product": "NRML"
    }
  ]
}
```

Alternatively, you can specify trading symbol and exchange instead of instrument token:

```json
{
  "triggerType": "single",
  "tradingSymbol": "NIFTY23JUNFUT",
  "exchange": "NFO",
  "triggerValues": [19500],
  "lastPrice": 19450,
  "orders": [
    {
      "transaction_type": "BUY",
      "quantity": 75,
      "price": 19500,
      "order_type": "LIMIT",
      "product": "NRML"
    }
  ]
}
```

**Response:**
```json
{
  "order_id": "gtt-123456",
  "status": "success",
  "message": "GTT order placed successfully"
}
```

**Parameters:**

**Instrument Identification (choose one method):**
- `instrumentToken`: Instrument token (recommended) - uniquely identifies the instrument without needing symbol and exchange
  
  OR
  
- `tradingSymbol`: Trading symbol of the instrument
- `exchange`: Exchange code (NSE, BSE, NFO, etc.)

**GTT Parameters (required):**
- `triggerType`: Type of trigger ("single" or "two-leg")
- `triggerValues`: Array of price points at which the GTT order triggers
- `lastPrice`: Current price of the instrument
- `orders`: Array of order objects to be placed when triggered

**Order Object Parameters:**
- `transaction_type`: Order side (BUY, SELL)
- `quantity`: Order quantity
- `price`: Order price
- `order_type`: Type of order (usually LIMIT for GTT)
- `product`: Product type (NRML, MIS, CNC)

### Get Orders

Retrieves a list of all placed orders, including both regular and GTT orders.

**Endpoint:** `GET /api/orders`

**Response:**
```json
{
  "success": true,
  "orders": [
    {
      "id": 1,
      "order_id": "230430000012345",
      "order_type": "MARKET",
      "gtt_type": "",
      "status": "COMPLETE",
      "message": "Order executed successfully",
      "trading_symbol": "NIFTY23JUNFUT",
      "exchange": "NFO",
      "side": "BUY",
      "quantity": 75,
      "price": 19450.25,
      "trigger_price": 0,
      "product": "NRML",
      "validity": "DAY",
      "disclosed_qty": 0,
      "tag": "GoHustle",
      "user_id": "",
      "placed_at": "2023-06-15T10:30:00+05:30",
      "paper_trading": false
    }
  ]
}
```

**Response Fields:**
- `id`: Database ID of the order record
- `order_id`: Zerodha order ID or GTT trigger ID
- `order_type`: Type of order (MARKET, LIMIT, SL, SL-M, GTT)
- `gtt_type`: For GTT orders, can be "single" or "oco" (one-cancels-other)
- `status`: Current status of the order
- `message`: Status message from the broker
- `trading_symbol`: Trading symbol of the instrument
- `exchange`: Exchange code (NSE, BSE, NFO, etc.)
- `side`: Order side (BUY, SELL)
- `quantity`: Order quantity
- `price`: Order price
- `trigger_price`: Trigger price for SL and SL-M orders
- `product`: Product type (NRML, MIS, CNC)
- `validity`: Order validity (DAY, IOC)
- `disclosed_qty`: Disclosed quantity
- `tag`: Custom tag for the order
- `user_id`: User identifier (for multi-user setups)
- `placed_at`: Timestamp when the order was placed
- `paper_trading`: Whether this was a simulated paper trade

## Market Data

### Get Option Chain

Retrieves the option chain for a specific index.

**Endpoint:** `GET /api/option-chain?index=NIFTY&expiry=2023-06-29`

**Response:**
```json
{
  "success": true,
  "data": {
    "index": "NIFTY",
    "spot_price": 19450.25,
    "expiry": "2023-06-29",
    "timestamp": "2023-06-15T10:30:00+05:30",
    "options": [
      {
        "strike": 19400,
        "CE": {
          "instrument_token": "12345",
          "ltp": 120.5,
          "oi": 15000,
          "volume": 2500,
          "vwap": 118.75,
          "change": 2.5
        },
        "PE": {
          "instrument_token": "67890",
          "ltp": 80.25,
          "oi": 12000,
          "volume": 1800,
          "vwap": 82.5,
          "change": -1.75
        },
        "ce_pe_total": 200.75,
        "is_atm": true
      }
    ]
  }
}
```

### Get Time Series Metrics

Retrieves time series metrics for an index.

**Endpoint:** `GET /api/metrics?index=NIFTY&mode=historical&interval=5s&count=50`

**Response:**
```json
{
  "success": true,
  "message": "Time series metrics retrieved successfully",
  "data": {
    "mode": "historical",
    "index": "NIFTY",
    "interval": "5s",
    "requested_count": 50,
    "returned_count": 50,
    "metrics": [
      {
        "timestamp": "2023-06-15T10:30:00+05:30",
        "underlying_price": 19450.25,
        "synthetic_future": 19455.75,
        "lowest_straddle": 200.75,
        "atm_strike": 19400
      }
    ]
  }
}
```

## Positions

### Get Positions

Retrieves current positions.

**Endpoint:** `GET /api/positions`

**Response:**
```json
{
  "success": true,
  "data": {
    "net": [
      {
        "trading_symbol": "NIFTY23JUNFUT",
        "exchange": "NFO",
        "product": "NRML",
        "quantity": 75,
        "average_price": 19400.5,
        "last_price": 19450.25,
        "pnl": 3731.25,
        "unrealized": true,
        "multiplier": 1
      }
    ],
    "day": []
  }
}
```

## General Information

### Get General Info

Retrieves general market information.

**Endpoint:** `GET /api/general`

**Response:**
```json
{
  "success": true,
  "data": {
    "lot_sizes": {
      "NIFTY": 75,
      "BANKNIFTY": 30,
      "SENSEX": 20
    }
  }
}
```

## Error Responses

All API endpoints return a standard error format:

```json
{
  "success": false,
  "error": "Error message describing what went wrong"
}
```

Common HTTP status codes:
- `200 OK`: Request successful
- `400 Bad Request`: Invalid request parameters
- `401 Unauthorized`: Missing or invalid authentication
- `404 Not Found`: Resource not found
- `500 Internal Server Error`: Server-side error

## Tick Data Export API

The Tick Data Export API allows you to manage historical tick data, including listing available dates, exporting data to Parquet format, and deleting historical data.

### List Available Tick Dates

Retrieves a list of dates with available tick data. By default, returns data for both NIFTY and SENSEX indices, but can be filtered to a specific index.

**Endpoint:** `GET /api/ticks/dates`

**Query Parameters:**
- `index_name` (optional): Filter results to a specific index ("NIFTY" or "SENSEX")

**Example:** `GET /api/ticks/dates?index_name=NIFTY`

**Response:**
```json
{
  "success": true,
  "data": {
    "NIFTY": [
      {
        "date": "2025-05-01",
        "tick_count": 12500,
        "estimated_size_mb": 2.5
      },
      {
        "date": "2025-05-02",
        "tick_count": 13200,
        "estimated_size_mb": 2.7
      }
    ],
    "SENSEX": [
      {
        "date": "2025-05-01",
        "tick_count": 11800,
        "estimated_size_mb": 2.4
      },
      {
        "date": "2025-05-02",
        "tick_count": 12100,
        "estimated_size_mb": 2.5
      }
    ]
  }
}
```

**Response Fields:**
- `date`: Date in YYYY-MM-DD format
- `tick_count`: Number of tick data points available for that date
- `estimated_size_mb`: Estimated size of the data in megabytes

### Export Tick Data

Exports tick data for a specific index and date range to a Parquet file.

**Endpoint:** `POST /api/ticks/export`

**Request:**
```json
{
  "index_name": "NIFTY",
  "start_date": "2025-05-01",
  "end_date": "2025-05-06"
}
```

**Parameters:**
- `index_name`: Index name ("NIFTY" or "SENSEX")
- `start_date`: Start date in YYYY-MM-DD format
- `end_date`: End date in YYYY-MM-DD format

**Response:**
```json
{
  "success": true,
  "data": {
    "file_path": "NIFTY_2025-05-01_to_2025-05-06.parquet",
    "tick_count": 65000,
    "size_mb": 13.2
  }
}
```

**Response Fields:**
- `file_path`: Path to the exported Parquet file
- `tick_count`: Number of tick data points exported
- `size_mb`: Size of the exported file in megabytes

### Delete Tick Data

Deletes tick data for a specific index and date range from the database.

**Endpoint:** `POST /api/ticks/delete`

**Request:**
```json
{
  "index_name": "NIFTY",
  "start_date": "2025-05-01",
  "end_date": "2025-05-01"
}
```

**Parameters:**
- `index_name`: Index name ("NIFTY" or "SENSEX")
- `start_date`: Start date in YYYY-MM-DD format
- `end_date`: End date in YYYY-MM-DD format

**Response:**
```json
{
  "success": true,
  "data": {
    "deleted_count": 12500
  }
}
```

**Response Fields:**
- `deleted_count`: Number of tick data points deleted

### List Exported Files

Retrieves a list of all exported Parquet files.

**Endpoint:** `GET /api/ticks/files`

**Response:**
```json
{
  "success": true,
  "data": {
    "files": [
      {
        "file_name": "NIFTY_2025-05-01_to_2025-05-06.parquet",
        "size_mb": 13.2,
        "created_at": "2025-05-06T14:30:00+05:30"
      },
      {
        "file_name": "SENSEX_2025-05-01_to_2025-05-03.parquet",
        "size_mb": 7.5,
        "created_at": "2025-05-03T18:45:00+05:30"
      }
    ]
  }
}
```

**Response Fields:**
- `file_name`: Name of the exported Parquet file
- `size_mb`: Size of the file in megabytes
- `created_at`: Timestamp when the file was created

### Get Tick Samples

Retrieves sample tick data from an exported Parquet file.

**Endpoint:** `POST /api/ticks/samples`

**Request:**
```json
{
  "file_path": "NIFTY_2025-05-01_to_2025-05-06.parquet",
  "num_samples": 10
}
```

**Parameters:**
- `file_path`: Path to the exported Parquet file
- `num_samples`: Number of sample data points to retrieve

**Response:**
```json
{
  "success": true,
  "data": {
    "file_path": "NIFTY_2025-05-01_to_2025-05-06.parquet",
    "samples": [
      {
        "instrument_token": "256265",
        "exchange_unix_timestamp": 1714545000,
        "last_price": 19450.25,
        "open_interest": 150000,
        "volume_traded": 12500,
        "average_trade_price": 19448.75
      },
      {
        "instrument_token": "256265",
        "exchange_unix_timestamp": 1714545005,
        "last_price": 19451.50,
        "open_interest": 150100,
        "volume_traded": 12550,
        "average_trade_price": 19449.25
      }
    ]
  }
}
```

**Response Fields:**
- `file_path`: Path to the exported Parquet file
- `samples`: Array of sample tick data points
  - `instrument_token`: Instrument token
  - `exchange_unix_timestamp`: Exchange timestamp in Unix format
  - `last_price`: Last traded price
  - `open_interest`: Open interest
  - `volume_traded`: Volume traded
  - `average_trade_price`: Volume weighted average price

## Websocket API

Real-time data is available via WebSocket connection.

**Endpoint:** `ws://localhost:8080/ws`

**Subscription Message:**
```json
{
  "action": "subscribe",
  "channels": ["ticks.NIFTY", "metrics.NIFTY"]
}
```

**Sample Message:**
```json
{
  "channel": "ticks.NIFTY",
  "data": {
    "instrument_token": "256265",
    "timestamp": "2023-06-15T10:30:00+05:30",
    "last_price": 19450.25,
    "volume": 12500,
    "oi": 150000
  }
}
```
