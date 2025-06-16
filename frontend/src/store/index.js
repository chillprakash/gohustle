import { createStore } from 'vuex/dist/vuex.esm-bundler'
import api from '../services/api'

export default createStore({
  state: {
    user: null,
    token: localStorage.getItem('auth_token') || null,
    positions: [],
    orders: [],
    marketData: null,
    exportDates: [],
    expiries: {},
    loading: false,
    error: null
  },
  getters: {
    isAuthenticated: state => !!state.token,
    positions: state => state.positions,
    paperPositions: state => state.positions.filter(pos => pos.paperTrading),
    realPositions: state => state.positions.filter(pos => !pos.paperTrading),
    orders: state => state.orders,
    marketData: state => state.marketData,
    exportDates: state => state.exportDates,
    expiries: state => state.expiries,
    niftyExpiries: state => state.expiries.NIFTY || [],
    sensexExpiries: state => state.expiries.SENSEX || [],
    isLoading: state => state.loading,
    error: state => state.error
  },
  mutations: {
    SET_TOKEN(state, token) {
      state.token = token
      if (token) {
        localStorage.setItem('auth_token', token)
      } else {
        localStorage.removeItem('auth_token')
      }
    },
    SET_USER(state, user) {
      state.user = user
    },
    SET_POSITIONS(state, positions) {
      state.positions = positions
    },
    SET_ORDERS(state, orders) {
      state.orders = orders
    },
    SET_MARKET_DATA(state, data) {
      state.marketData = data
    },
    SET_EXPORT_DATES(state, dates) {
      state.exportDates = dates
    },
    SET_EXPIRIES(state, expiries) {
      state.expiries = expiries
    },
    SET_LOADING(state, isLoading) {
      state.loading = isLoading
    },
    SET_ERROR(state, error) {
      state.error = error
    }
  },
  actions: {
    async login({ commit }, credentials) {
      commit('SET_LOADING', true)
      commit('SET_ERROR', null)
      try {
        const response = await api.login(credentials)
        if (response.data && response.data.success) {
          commit('SET_TOKEN', response.data.data.token)
          // Assuming the user data is in the response, adjust according to your API
          commit('SET_USER', { username: credentials.username })
          return response.data
        } else {
          throw new Error(response.data?.message || 'Login failed')
        }
      } catch (error) {
        const errorMessage = error.response?.data?.message || error.message || 'Login failed'
        commit('SET_ERROR', errorMessage)
        throw new Error(errorMessage)
      } finally {
        commit('SET_LOADING', false)
      }
    },
    
    logout({ commit }) {
      commit('SET_TOKEN', null)
      commit('SET_USER', null)
      commit('SET_POSITIONS', [])
      commit('SET_ORDERS', [])
      commit('SET_MARKET_DATA', null)
    },
    
    async fetchPositions({ commit }, paperTrading = false) {
      commit('SET_LOADING', true)
      commit('SET_ERROR', null)
      try {
        const response = await api.getPositions(paperTrading)
        commit('SET_POSITIONS', response.data)
        return response.data
      } catch (error) {
        commit('SET_ERROR', error.response?.data?.message || 'Failed to fetch positions')
        throw error
      } finally {
        commit('SET_LOADING', false)
      }
    },
    
    async fetchOrders({ commit }) {
      commit('SET_LOADING', true)
      commit('SET_ERROR', null)
      try {
        const response = await api.getOrders()
        commit('SET_ORDERS', response.data)
        return response.data
      } catch (error) {
        commit('SET_ERROR', error.response?.data?.message || 'Failed to fetch orders')
        throw error
      } finally {
        commit('SET_LOADING', false)
      }
    },
    
    async fetchMarketData({ commit }) {
      commit('SET_LOADING', true)
      commit('SET_ERROR', null)
      try {
        const response = await api.getMarketData()
        commit('SET_MARKET_DATA', response.data)
        return response.data
      } catch (error) {
        commit('SET_ERROR', error.response?.data?.message || 'Failed to fetch market data')
        throw error
      } finally {
        commit('SET_LOADING', false)
      }
    },
    
    async fetchExportDates({ commit }) {
      commit('SET_LOADING', true)
      commit('SET_ERROR', null)
      try {
        const response = await api.getAvailableDates()
        commit('SET_EXPORT_DATES', response.data)
        return response.data
      } catch (error) {
        commit('SET_ERROR', error.response?.data?.message || 'Failed to fetch export dates')
        throw error
      } finally {
        commit('SET_LOADING', false)
      }
    },
    
    async fetchExpiries({ commit }) {
      commit('SET_LOADING', true)
      commit('SET_ERROR', null)
      try {
        const response = await api.getExpiries()
        if (response.data && response.data.success) {
          // The API returns { NIFTY: ["12-06-2025"], SENSEX: ["10-06-2025"] }
          // Convert it to the format expected by the UI
          const formattedExpiries = {};
          Object.entries(response.data.data).forEach(([symbol, dates]) => {
            formattedExpiries[symbol] = dates || [];
          });
          
          commit('SET_EXPIRIES', formattedExpiries)
          return formattedExpiries;
        } else {
          throw new Error(response.data?.message || 'Failed to fetch expiry dates')
        }
      } catch (error) {
        const errorMsg = error.response?.data?.message || error.message || 'Failed to fetch expiry dates'
        commit('SET_ERROR', errorMsg)
        throw new Error(errorMsg)
      } finally {
        commit('SET_LOADING', false)
      }
    },
    
    async exportData({ commit }, dateRange) {
      commit('SET_LOADING', true)
      commit('SET_ERROR', null)
      try {
        const response = await api.exportData(dateRange)
        return response.data
      } catch (error) {
        commit('SET_ERROR', error.response?.data?.message || 'Failed to export data')
        throw error
      } finally {
        commit('SET_LOADING', false)
      }
    },
    
    async deleteData({ commit }, dateRange) {
      commit('SET_LOADING', true)
      commit('SET_ERROR', null)
      try {
        const response = await api.deleteData(dateRange)
        return response.data
      } catch (error) {
        commit('SET_ERROR', error.response?.data?.message || 'Failed to delete data')
        throw error
      } finally {
        commit('SET_LOADING', false)
      }
    }
  }
})
