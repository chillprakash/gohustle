import axios from 'axios';

const API_URL = process.env.VUE_APP_API_URL || 'http://localhost:8080/api';

const apiClient = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  },
  timeout: 10000
});

// Add request interceptor for authentication
apiClient.interceptors.request.use(config => {
  const token = localStorage.getItem('auth_token');
  if (token) {
    config.headers['Authorization'] = `Bearer ${token}`;
  }
  return config;
});

// API service methods
export default {
  // Authentication
  login(credentials) {
    return apiClient.post('/auth/login', credentials);
  },
  
  // Market Data
  getMarketData() {
    return apiClient.get('/market/data');
  },
  
  // Option Chain
  getOptionChain(symbol, expiry, strikes = 20) {
    return apiClient.get('/option-chain', {
      params: {
        index: symbol,
        expiry: expiry,
        strikes: strikes
      }
    });
  },
  
  // Get all expiries for all symbols
  getExpiries() {
    return apiClient.get('/expiries');
  },
  
  // Get Expiry Dates for a specific symbol
  getExpiryDates(symbol) {
    return apiClient.get(`/optionchain/expiry/${symbol}`);
  },
  
  // Positions
  getPositions(paperTrading = false) {
    return apiClient.get(`/positions?paper_trading=${paperTrading}`);
  },
  
  // Orders
  placeOrder(orderData) {
    return apiClient.post('/orders', orderData);
  },
  
  getOrders() {
    return apiClient.get('/orders');
  },
  
  // Tick Data Export
  getAvailableDates() {
    return apiClient.get('/export/dates');
  },
  
  exportData(dateRange) {
    return apiClient.post('/export/data', dateRange);
  },
  
  deleteData(dateRange) {
    return apiClient.delete('/export/data', { data: dateRange });
  }
};
