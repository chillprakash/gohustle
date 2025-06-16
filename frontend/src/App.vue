<template>
  <div id="app">
    <!-- Top Bar with Market Data -->
    <div class="top-bar">
      <div class="market-indicators">
        <!-- Time -->
        <div class="indicator time">
          <span class="value">{{ currentTime }}</span>
        </div>
        
        <!-- NIFTY -->
        <div class="indicator" :class="{ 'positive': niftyChange >= 0, 'negative': niftyChange < 0 }">
          <span class="label">NIFTY</span>
          <span class="value">{{ formatNumber(niftyPrice) }}</span>
          <span class="change">
            {{ niftyChange >= 0 ? '+' : '' }}{{ formatNumber(niftyChange) }} ({{ niftyChangePercent >= 0 ? '+' : '' }}{{ niftyChangePercent.toFixed(2) }}%)
          </span>
        </div>
        
        <!-- SENSEX -->
        <div class="indicator" :class="{ 'positive': sensexChange >= 0, 'negative': sensexChange < 0 }">
          <span class="label">SENSEX</span>
          <span class="value">{{ formatNumber(sensexPrice) }}</span>
          <span class="change">
            {{ sensexChange >= 0 ? '+' : '' }}{{ formatNumber(sensexChange) }} ({{ sensexChangePercent >= 0 ? '+' : '' }}{{ sensexChangePercent.toFixed(2) }}%)
          </span>
        </div>
        
        <!-- BANKNIFTY -->
        <div class="indicator" :class="{ 'positive': bankniftyChange >= 0, 'negative': bankniftyChange < 0 }">
          <span class="label">BANKNIFTY</span>
          <span class="value">{{ formatNumber(bankniftyPrice) }}</span>
          <span class="change">
            {{ bankniftyChange >= 0 ? '+' : '' }}{{ formatNumber(bankniftyChange) }} ({{ bankniftyChangePercent >= 0 ? '+' : '' }}{{ bankniftyChangePercent.toFixed(2) }}%)
          </span>
        </div>
      </div>
      
      <!-- User Controls -->
      <div class="user-controls">
        <div v-if="isAuthenticated" class="dropdown">
          <button 
            class="user-button" 
            type="button" 
            id="userDropdown" 
            data-bs-toggle="dropdown" 
            aria-expanded="false"
          >
            <span class="username">{{ user?.username || 'User' }}</span>
            <span>▼</span>
          </button>
          <ul class="dropdown-menu dropdown-menu-end" aria-labelledby="userDropdown">
            <li><a class="dropdown-item" href="#" @click="logout">Logout</a></li>
          </ul>
        </div>
        <router-link v-else to="/login" class="login-button">
          Login
        </router-link>
      </div>
    </div>

    <!-- Navigation Tabs -->
    <nav class="app-nav">
      <div class="nav-container">
        <router-link 
          to="/optionchain" 
          class="nav-tab"
          :class="{ 'active': $route.path === '/optionchain' }"
        >
          Option Chain
        </router-link>
        <!-- Add more navigation tabs here as needed -->
      </div>
    </nav>

    <!-- Main Content -->
    <main class="main-content">
      <router-view />
    </main>

    <!-- Footer -->
    <footer class="bg-light text-center text-muted py-3 mt-auto">
      <div class="container">
        <p class="mb-0">&copy; {{ new Date().getFullYear() }} GoHustle. All rights reserved.</p>
      </div>
    </footer>
  </div>
</template>

<script>
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { useStore } from 'vuex'
import { useRouter } from 'vue-router/dist/vue-router.esm-bundler'

export default {
  name: 'App',
  setup() {
    const store = useStore()
    const router = useRouter()
    
    // Market data state (mock data - replace with real API calls)
    const currentTime = ref('')
    const niftyPrice = ref(23456.78)
    const niftyChange = ref(123.45)
    const niftyChangePercent = ref(0.53)
    const sensexPrice = ref(78543.21)
    const sensexChange = ref(234.56)
    const sensexChangePercent = ref(0.30)
    const bankniftyPrice = ref(51234.56)
    const bankniftyChange = ref(345.67)
    const bankniftyChangePercent = ref(0.68)
    
    // Computed properties
    const isAuthenticated = computed(() => store.getters.isAuthenticated)
    const user = computed(() => store.state.user)
    
    // Methods
    const updateTime = () => {
      const now = new Date()
      currentTime.value = now.toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: true
      })
    }
    
    const fetchMarketData = async () => {
      try {
        // TODO: Replace with actual API call to fetch market data
        // For now, using mock data with small random variations
        const randomize = (value, maxChange) => {
          const change = (Math.random() * 2 - 1) * maxChange
          return Math.max(0, value + change)
        }
        
        niftyPrice.value = randomize(23456.78, 50)
        niftyChange.value = randomize(123.45, 5)
        niftyChangePercent.value = (niftyChange.value / (niftyPrice.value - niftyChange.value)) * 100
        
        sensexPrice.value = randomize(78543.21, 100)
        sensexChange.value = randomize(234.56, 10)
        sensexChangePercent.value = (sensexChange.value / (sensexPrice.value - sensexChange.value)) * 100
        
        bankniftyPrice.value = randomize(51234.56, 75)
        bankniftyChange.value = randomize(345.67, 8)
        bankniftyChangePercent.value = (bankniftyChange.value / (bankniftyPrice.value - bankniftyChange.value)) * 100
      } catch (error) {
        console.error('Error fetching market data:', error)
      }
    }
    
    const formatNumber = (num) => {
      if (num === undefined || num === null) return '--'
      return num.toLocaleString('en-IN', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      })
    }
    
    const logout = async () => {
      try {
        await store.dispatch('logout')
        router.push('/login')
      } catch (error) {
        console.error('Logout failed:', error)
      }
    }
    
    // Lifecycle hooks
    onMounted(() => {
      // Update time immediately and set interval
      updateTime()
      const timeInterval = setInterval(updateTime, 1000)
      
      // Fetch market data immediately and set interval (e.g., every 5 seconds)
      fetchMarketData()
      const marketDataInterval = setInterval(fetchMarketData, 5000)
      
      // Cleanup intervals on component unmount
      onBeforeUnmount(() => {
        clearInterval(timeInterval)
        clearInterval(marketDataInterval)
      })
    })
    
    return {
      // Data
      currentTime,
      niftyPrice,
      niftyChange,
      niftyChangePercent,
      sensexPrice,
      sensexChange,
      sensexChangePercent,
      bankniftyPrice,
      bankniftyChange,
      bankniftyChangePercent,
      
      // Computed
      isAuthenticated,
      user,
      
      // Methods
      formatNumber,
      logout
    }
  }
}
</script>

<style>
@import 'bootstrap/dist/css/bootstrap.min.css';

:root {
  --bg-primary: #ffffff;
  --bg-secondary: #f8f9fa;
  --text-primary: #212529;
  --text-secondary: #6c757d;
  --positive: #198754;
  --negative: #dc3545;
  --border-color: #dee2e6;
  --hover-bg: rgba(0, 0, 0, 0.03);
  --active-bg: rgba(0, 0, 0, 0.06);
  --font-mono: 'Roboto Mono', 'Courier New', monospace;
}

* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

body, html {
  margin: 0;
  padding: 0;
  height: 100%;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  background-color: var(--bg-primary);
  color: var(--text-primary);
  font-size: 13px;
  line-height: 1.5;
}

#app {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
  background-color: var(--bg-primary);
  color: var(--text-primary);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
  font-size: 13px;
  line-height: 1.5;
}

/* Top Bar */
.top-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  height: 36px;
  background-color: var(--bg-secondary);
  border-bottom: 1px solid var(--border-color);
  padding: 0 12px;
  color: var(--text-secondary);
  font-weight: 500;
}

.market-indicators {
  display: flex;
  align-items: center;
  gap: 20px;
  overflow-x: auto;
  scrollbar-width: none;
  -ms-overflow-style: none;
}

.market-indicators::-webkit-scrollbar {
  display: none;
}

.indicator {
  display: flex;
  align-items: center;
  gap: 6px;
  white-space: nowrap;
  padding: 4px 8px;
  border-radius: 4px;
  transition: background-color 0.2s;
}

.indicator.time {
  background-color: rgba(0, 0, 0, 0.03);
  font-family: monospace;
}

.indicator .label {
  color: var(--text-secondary);
  font-weight: 500;
}

.indicator .value {
  color: var(--text-primary);
  font-weight: 600;
}

.indicator .change {
  font-size: 11px;
  font-weight: 500;
  margin-left: 4px;
}

.indicator.positive .change {
  color: var(--positive);
}

.indicator.negative .change {
  color: var(--negative);
}

/* User Controls */
.user-controls {
  display: flex;
  align-items: center;
  margin-left: auto;
  padding-left: 12px;
  border-left: 1px solid var(--border-color);
  height: 100%;
}

.user-button, .login-button {
  display: flex;
  align-items: center;
  gap: 6px;
  background: none;
  border: none;
  color: var(--text-primary);
  font-size: 12px;
  font-weight: 500;
  padding: 4px 8px;
  border-radius: 4px;
  cursor: pointer;
  text-decoration: none;
}

.user-button:hover, .login-button:hover {
  background-color: var(--hover-bg);
}

/* Navigation Tabs */
.app-nav {
  height: 36px;
  background-color: var(--bg-secondary);
  border-bottom: 1px solid var(--border-color);
  padding: 0 12px;
}

.nav-container {
  height: 100%;
  display: flex;
  align-items: center;
}

.nav-tab {
  display: flex;
  align-items: center;
  height: 100%;
  padding: 0 16px;
  color: var(--text-secondary);
  text-decoration: none;
  font-weight: 500;
  font-size: 13px;
  transition: all 0.2s;
  border-bottom: 2px solid transparent;
}

.nav-tab:hover {
  color: var(--text-primary);
  background-color: var(--hover-bg);
}

.nav-tab.active {
  color: #0d6efd;
  border-bottom-color: #0d6efd;
  font-weight: 600;
}

/* Main Content */
.main-content {
  flex: 1;
  padding: 12px;
  overflow-y: auto;
  background-color: var(--bg-primary);
}

/* Dropdown Menu */
.dropdown-menu {
  background-color: var(--bg-primary);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
  padding: 4px 0;
  min-width: 160px;
}

.dropdown-item {
  color: var(--text-primary);
  font-size: 13px;
  padding: 6px 16px;
  transition: background-color 0.2s;
}

.dropdown-item:hover {
  background-color: var(--hover-bg);
  color: var(--text-primary);
}

.dropdown-divider {
  border-top: 1px solid var(--border-color);
  margin: 4px 0;
}

/* Responsive Adjustments */
@media (max-width: 768px) {
  .market-indicators {
    gap: 12px;
  }
  
  .indicator {
    padding: 4px 6px;
  }
  
  .indicator .label {
    display: none;
  }
  
  .top-bar, .app-nav {
    padding: 0 8px;
  }
  
  .nav-tab {
    padding: 0 12px;
    font-size: 12px;
  }
}

#app {
  font-family: 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', 'Fira Sans', 'Droid Sans', 'Helvetica Neue', sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  color: #212529;
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}

/* Navbar Customization */
.navbar {
  min-height: var(--navbar-height);
  background-color: #2c3e50 !important;
  border-bottom: 1px solid rgba(255, 255, 255, 0.05);
  padding: 0;
  box-shadow: none;
}

.navbar-nav {
  --bs-nav-link-padding-x: 0.5rem;
  --bs-nav-link-padding-y: 0.15rem;
  --bs-nav-link-font-size: var(--nav-link-size);
  --bs-nav-link-font-weight: 500;
  --bs-nav-link-color: rgba(255, 255, 255, 0.9);
  --bs-nav-link-hover-color: #fff;
  --bs-nav-link-disabled-color: rgba(255, 255, 255, 0.5);
}

.navbar-toggler {
  padding: 0.1rem 0.3rem;
  font-size: 0.8rem;
  line-height: 1;
  margin-left: 0.3rem;
  border-width: 1px;
}

.navbar-toggler-icon {
  width: 0.9rem;
  height: 0.9rem;
  background-size: 85%;
  margin: 0 auto;
}

.nav-link {
  padding: var(--bs-nav-link-padding-y) var(--bs-nav-link-padding-x);
  font-size: var(--bs-nav-link-font-size);
  font-weight: var(--bs-nav-link-font-weight);
  color: var(--bs-nav-link-color);
  text-decoration: none;
  transition: color 0.1s ease;
  white-space: nowrap;
  display: flex;
  align-items: center;
  height: 100%;
}

.nav-link:hover,
.nav-link:focus,
.nav-link.active {
  color: var(--bs-nav-link-hover-color);
  background-color: rgba(255, 255, 255, 0.1);
}

.nav-link.active {
  font-weight: 600;
}

/* Compact dropdown */
.dropdown-menu {
  font-size: 0.8125rem;
  min-width: 9rem;
  padding: 0.15rem 0;
  margin: 0.1rem 0 0;
  border: 1px solid rgba(0, 0, 0, 0.1);
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.dropdown-item {
  padding: 0.2rem 0.75rem;
  font-size: 0.8125rem;
  line-height: 1.4;
}

.dropdown-item:hover {
  background-color: #f8f9fa;
}

.dropdown-divider {
  margin: 0.15rem 0;
  border-top: 1px solid rgba(0, 0, 0, 0.05);
}

/* Compact buttons */
.btn {
  padding: 0.15rem 0.6rem;
  font-size: 0.8125rem;
  line-height: 1.4;
  border-radius: var(--nav-border-radius);
  transition: all 0.1s ease;
}

.btn-sm {
  padding: 0.1rem 0.5rem;
  font-size: 0.75rem;
  line-height: 1.3;
}

.btn-outline-light {
  border-color: rgba(255, 255, 255, 0.3);
}

/* Adjust container padding */
.container-fluid {
  padding-left: 1rem;
  padding-right: 1rem;
}

/* Make the navbar items vertically centered */
.navbar-nav {
  align-items: center;
}

/* Compact form controls */
.form-control, .form-select {
  height: calc(1.5em + 0.5rem + 2px);
  padding: 0.25rem 0.5rem;
  font-size: 0.875rem;
  line-height: 1.5;
  border-radius: 0.2rem;
}

/* Main Content */
main {
  flex: 1;
  padding: 2rem 1rem;
}

/* Footer */
footer {
  background-color: #f8f9fa;
  border-top: 1px solid #e9ecef;
}

/* Responsive Adjustments */
@media (max-width: 991.98px) {
  .navbar-collapse {
    padding: 1rem 0;
  }
  
  .nav-link {
    margin: 0.25rem 0;
    padding: 0.5rem 1rem;
  }
  
  .dropdown-menu {
    border: none;
    box-shadow: none;
    padding: 0;
  }
  
  .dropdown-item {
    padding: 0.5rem 1.5rem;
  }
}

/* Animation for page transitions */
.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.2s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}
</style>
