<template>
  <div class="option-chain-container">
    <div class="compact-controls">
      <div class="control-group">
        <select 
          id="symbol" 
          v-model="selectedSymbol" 
          @change="onSymbolChange"
          :disabled="loading"
          class="form-select form-select-sm"
        >
          <option 
            v-for="symbol in availableSymbols" 
            :key="symbol" 
            :value="symbol"
          >
            {{ symbol }}
          </option>
          <option v-if="!availableSymbols.length" disabled>Loading...</option>
        </select>
      </div>
      
      <div class="filter-group">
        <label for="expiry">Expiry:</label>
        <select 
          id="expiry" 
          v-model="selectedExpiry" 
          @change="fetchOptionChain"
          :disabled="!selectedSymbol || !currentExpiries.length"
          class="form-select form-select-sm"
        >
          <option 
            v-for="expiry in currentExpiries" 
            :key="expiry" 
            :value="expiry"
          >
            {{ formatExpiryDate(expiry) }}
          </option>
          <option v-if="!currentExpiries.length" disabled>Loading...</option>
        </select>
      </div>
      
      <div class="filter-group">
        <button 
          class="btn btn-sm btn-outline-primary refresh-btn"
          @click="fetchOptionChain(true)"
          :disabled="loading || !selectedExpiry" 
        >
          <span v-if="loading" class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
          <span v-else>⟳ Refresh</span>
        </button>
      </div>
      
      <div v-if="lastFetched" class="last-updated">
        Last updated: {{ formatLastUpdated(lastFetched) }}
      </div>
    </div>
    
    <!-- Meta Data Display -->
    <div class="meta-data">
      <div v-for="(value, key) in metaData" :key="key" class="meta-item">
        <strong>{{ key }}:</strong> {{ value }}
      </div>
    </div>
    
    <div v-if="loading" class="loading">
      Loading option chain data...
    </div>
    
    <div v-else-if="error" class="error">
      {{ error }}
    </div>
    
    <div v-else-if="optionChainData.length === 0" class="no-data">
      No data available. Select a symbol and expiry to view the option chain.
    </div>
    <div v-else class="option-chain-table-container">
      <div v-if="metaData.index" class="option-chain-metadata mb-3">
        <div class="d-flex justify-content-between">
          <div>
            <strong>Index:</strong> {{ metaData.index }}
            <span class="mx-2">|</span>
            <strong>Expiry:</strong> {{ metaData.expiry }}
            <span class="mx-2">|</span>
            <strong>Underlying:</strong> {{ metaData.underlyingPrice }}
            <span class="mx-2">|</span>
            <strong>ATM Strike:</strong> {{ metaData.atmStrike }}
          </div>
          <div v-if="lastFetched">
            Last updated: {{ formatLastUpdated(lastFetched) }}
          </div>
        </div>
      </div>
      
      <div class="option-chain-table">
        <table v-if="optionChainData.length > 0" class="table table-sm table-hover">
          <thead>
            <tr>
              <th colspan="3" class="text-center">PUTS</th>
              <th class="text-center">Strike</th>
              <th colspan="3" class="text-center">CALLS</th>
              <th class="text-center">Total</th>
            </tr>
            <tr>
              <th class="text-end value-cell">OI</th>
              <th class="text-end value-cell">Vol</th>
              <th class="text-end value-cell">LTP</th>
              <th class="text-center strike-cell">{{ selectedSymbol }} @ {{ metaData.underlyingPrice }}</th>
              <th class="text-start value-cell">LTP</th>
              <th class="text-start value-cell">Vol</th>
              <th class="text-start value-cell">OI</th>
              <th class="text-center value-cell">CE+PE</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in optionChainData" 
                :key="row.strike" 
                :class="{ 'atm-row': row.is_atm }"
                class="hover-effect">
              <!-- Puts (PE) -->
              <td :class="['text-end value-cell', row.PE?.oi ? `bg-oi-pe-${formatOI(row.PE.oi, 'PE').intensity}` : '']">
                {{ row.PE?.formatted_oi || formatNumber(row.PE?.oi) || '0' }}
              </td>
              <td :class="['text-end value-cell', row.PE?.volume ? `bg-volume-pe-${getVolumeIntensity(row.PE.volume, 'PE')}` : '']">
                {{ formatNumber(row.PE?.volume) || '0' }}
              </td>
              <td class="text-end value-cell" :class="{ 'text-success': row.PE?.change > 0, 'text-danger': row.PE?.change < 0 }">
                {{ row.PE?.ltp ? row.PE.ltp.toFixed(2) : '-' }}
              </td>
              
              <!-- Strike Price -->
              <td class="text-center fw-bold strike-cell">{{ row.strike }}</td>
              
              <!-- Calls (CE) -->
              <td class="text-start value-cell" :class="{ 'text-success': row.CE?.change > 0, 'text-danger': row.CE?.change < 0 }">
                {{ row.CE?.ltp ? row.CE.ltp.toFixed(2) : '-' }}
              </td>
              <td :class="['text-start value-cell', row.CE?.volume ? `bg-volume-ce-${getVolumeIntensity(row.CE.volume, 'CE')}` : '']">
                {{ formatNumber(row.CE?.volume) || '0' }}
              </td>
              <td :class="['text-start value-cell', row.CE?.oi ? `bg-oi-ce-${formatOI(row.CE.oi, 'CE').intensity}` : '']">
                {{ row.CE?.formatted_oi || formatNumber(row.CE?.oi) || '0' }}
              </td>
              <td class="text-center fw-bold value-cell">
                {{ calculateTotal(row) }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, computed, watch, onMounted, onActivated } from 'vue';
import { useStore } from 'vuex';
import api from '@/services/api';

export default {
  name: 'OptionChain',
  setup() {
    const store = useStore();
    
    // State
    const selectedSymbol = ref('NIFTY');
    const selectedExpiry = ref('');
    const optionChainData = ref([]);
    const metaData = ref({});
    const loading = ref(false);
    const error = ref(null);
    const lastFetched = ref(null);
    
    // Computed
    const availableSymbols = computed(() => {
      return Object.keys(store.getters.expiries).sort();
    });
    
    const currentExpiries = computed(() => {
      return store.getters.expiries[selectedSymbol.value] || [];
    });
    
    // Watchers
    watch(selectedSymbol, async (newVal, oldVal) => {
      if (newVal !== oldVal) {
        selectedExpiry.value = '';
        optionChainData.value = [];
        metaData.value = {};
        
        // Only fetch expiries if we don't have them already
        if (!store.getters.expiries[newVal]?.length) {
          await store.dispatch('fetchExpiries');
        }
        
        // If we have expiries for this symbol, select the first one
        if (currentExpiries.value.length > 0) {
          selectedExpiry.value = currentExpiries.value[0];
        }
      }
    });
    
    // Only watch for expiry changes to refresh the table
    watch(selectedExpiry, (newVal) => {
      if (newVal) {
        fetchOptionChain();
      } else {
        optionChainData.value = [];
        metaData.value = {};
      }
    });
    
    // Function to fetch expiry dates
    const fetchExpiryDates = async () => {
      try {
        await store.dispatch('fetchExpiries');
        // Auto-select first expiry if none selected
        if (currentExpiries.value.length > 0 && !selectedExpiry.value) {
          selectedExpiry.value = currentExpiries.value[0];
        }
      } catch (err) {
        console.error('Error fetching expiry dates:', err);
        error.value = 'Failed to load expiry dates. Please try again.';
      }
    };
    
    // Refresh data when tab is activated
    onActivated(() => {
      // Only refresh if we have a selected expiry and it's been more than 5 seconds since last fetch
      if (selectedExpiry.value) {
        const now = Date.now();
        if (!lastFetched.value || (now - lastFetched.value > 5000)) {
          fetchOptionChain(true);
        }
      } else if (currentExpiries.value.length > 0 && !selectedExpiry.value) {
        // If we have expiries but none selected, select the first one
        selectedExpiry.value = currentExpiries.value[0];
      } else if (Object.keys(store.getters.expiries).length === 0) {
        // If we don't have any expiries at all, fetch them
        fetchExpiryDates();
      }
    });
    
    // Initial setup
    onMounted(() => {
      // Only fetch expiries if we don't have them already
      if (Object.keys(store.getters.expiries).length === 0) {
        fetchExpiryDates().then(() => {
          if (currentExpiries.value.length > 0 && !selectedExpiry.value) {
            selectedExpiry.value = currentExpiries.value[0];
          }
        });
      } else if (currentExpiries.value.length > 0 && !selectedExpiry.value) {
        selectedExpiry.value = currentExpiries.value[0];
      }
    });
    
    // Methods
    const fetchOptionChain = async (force = false) => {
      if (!selectedSymbol.value || !selectedExpiry.value) return;
      
      // Skip if we've fetched recently (within 5 seconds) and not forcing
      const now = Date.now();
      if (!force && lastFetched.value && (now - lastFetched.value < 5000)) {
        return;
      }
      
      loading.value = true;
      error.value = null;
      
      try {
        const response = await api.getOptionChain(selectedSymbol.value, selectedExpiry.value, 20);
        console.log('Option Chain API Response:', response);
        
        if (response.data && response.data.success) {
          // Update the option chain data with the chain array
          optionChainData.value = response.data.data?.chain || [];
          // Store additional metadata
          metaData.value = {
            index: response.data.data?.index,
            expiry: response.data.data?.expiry,
            underlyingPrice: response.data.data?.underlying_price,
            atmStrike: response.data.data?.atm_strike,
            timestamp: response.data.data?.timestamp
          };
          lastFetched.value = now;
          console.log('Processed option chain data:', optionChainData.value);
        } else {
          throw new Error(response.data?.message || 'Failed to fetch option chain data');
        }
      } catch (err) {
        console.error('Error fetching option chain:', err);
        error.value = 'Failed to load option chain. ' + (err.response?.data?.message || err.message || 'Please try again.');
      } finally {
        loading.value = false;
      }
    };
    
    // Refresh data when tab is activated
    onActivated(() => {
      // Only refresh if we have a selected expiry and it's been more than 5 seconds since last fetch
      if (selectedExpiry.value) {
        const now = Date.now();
        if (!lastFetched.value || (now - lastFetched.value > 5000)) {
          fetchOptionChain(true);
        }
      } else if (currentExpiries.value.length > 0 && !selectedExpiry.value) {
        // If we have expiries but none selected, select the first one
        selectedExpiry.value = currentExpiries.value[0];
      } else if (Object.keys(store.getters.expiries).length === 0) {
        // If we don't have any expiries at all, fetch them
        fetchExpiryDates();
      }
    });
    
    // Initial setup
    onMounted(() => {
      // Only fetch expiries if we don't have them already
      if (Object.keys(store.getters.expiries).length === 0) {
        fetchExpiryDates();
      } else if (currentExpiries.value.length > 0 && !selectedExpiry.value) {
        selectedExpiry.value = currentExpiries.value[0];
      }
    });
    
    const formatExpiryDate = (dateStr) => {
      // Return the date as-is from the API
      return dateStr || '';
    };
    
    const formatLastUpdated = (timestamp) => {
      if (!timestamp) return '';
      const date = new Date(timestamp);
      return date.toLocaleTimeString();
    };
    
    const formatStrike = (item) => {
      return `${item.strike}(${item.index})`;
    };
    
    // Format OI with K/M suffix and get intensity level (1-6) for coloring
    const formatOI = (oi, type) => {
      if (oi === undefined || oi === null || oi === 0) {
        return { display: '-', intensity: 0 };
      }
      
      let display;
      if (oi >= 1000000) {
        display = `${(oi / 1000000).toFixed(1)}M`;
      } else if (oi >= 1000) {
        display = `${(oi / 1000).toFixed(1)}K`;
      } else {
        display = oi.toString();
      }
      
      // Calculate intensity level (1-6) based on value's position in the range
      let intensity = 0;
      if (!optionChainData.value || !optionChainData.value.length) {
        return { display, intensity };
      }
      
      try {
        if (type === 'CE') {
          const ceOIs = optionChainData.value.map(row => row.CE?.oi || 0).filter(oi => oi > 0);
          const maxCeOi = ceOIs.length ? Math.max(...ceOIs) : 0;
          intensity = maxCeOi > 0 ? Math.min(6, Math.max(1, Math.ceil((oi / maxCeOi) * 6))) : 0;
        } else {
          const peOIs = optionChainData.value.map(row => row.PE?.oi || 0).filter(oi => oi > 0);
          const maxPeOi = peOIs.length ? Math.max(...peOIs) : 0;
          intensity = maxPeOi > 0 ? Math.min(6, Math.max(1, Math.ceil((oi / maxPeOi) * 6))) : 0;
        }
      } catch (e) {
        console.error('Error calculating OI intensity:', e);
        intensity = 0;
      }
      
      return { display, intensity };
    };

    // Get volume intensity level (1-4) for coloring
    const getVolumeIntensity = (volume, type) => {
      if (!volume || !optionChainData.value || !optionChainData.value.length) return 0;
      
      try {
        let maxVolume = 0;
        if (type === 'CE') {
          const ceVolumes = optionChainData.value.map(row => row.CE?.volume || 0).filter(v => v > 0);
          maxVolume = ceVolumes.length ? Math.max(...ceVolumes) : 0;
        } else {
          const peVolumes = optionChainData.value.map(row => row.PE?.volume || 0).filter(v => v > 0);
          maxVolume = peVolumes.length ? Math.max(...peVolumes) : 0;
        }
        
        return maxVolume > 0 ? Math.min(4, Math.max(1, Math.ceil((volume / maxVolume) * 4))) : 0;
      } catch (e) {
        console.error('Error calculating volume intensity:', e);
        return 0;
      }
    };
    
    // Format number with K, L, M suffixes for better readability
    const formatNumber = (num) => {
      if (num === undefined || num === null) return '0';
      
      // Convert string to number if needed
      const value = typeof num === 'string' ? parseFloat(num) : num;
      
      // Handle non-numeric values
      if (isNaN(value)) return '0';
      
      // Format based on the value
      if (value >= 10000000) {
        // For 1Cr+ (1,00,00,000+), show in Crores with 1 decimal
        return (value / 10000000).toFixed(1) + 'Cr';
      } else if (value >= 100000) {
        // For 1L+ (1,00,000+), show in Lakhs with 1 decimal
        return (value / 100000).toFixed(1) + 'L';
      } else if (value >= 1000) {
        // For 1K+ (1,000+), show in Thousands with 1 decimal
        return (value / 1000).toFixed(1) + 'K';
      }
      
      // For numbers less than 1,000, return as is
      return Math.round(value).toString();
    };
    
    const formatPrice = (price) => {
      if (price === undefined || price === null) return '-';
      return price.toFixed(2);
    };
    
    const formatChange = (change) => {
      if (change === undefined || change === null) return '-';
      const sign = change > 0 ? '+' : '';
      const color = change > 0 ? 'text-success' : change < 0 ? 'text-danger' : '';
      return `<span class="${color}">${sign}${change.toFixed(2)}</span>`;
    };
    
    // Calculate total of CE and PE LTP
    const calculateTotal = (row) => {
      const ceLtp = parseFloat(row.CE?.ltp) || 0;
      const peLtp = parseFloat(row.PE?.ltp) || 0;
      const total = ceLtp + peLtp;
      return total > 0 ? total.toFixed(2) : '-';
    };
    
    const getLtpColor = (item, type) => {
      const ltp = item[`${type}_ltp`];
      const ohlc = item[`${type}_ohlc`];
      
      if (!ohlc) return '';
      
      const [open] = ohlc.split('|').map(val => parseFloat(val));
      const ltpValue = parseFloat(ltp);
      
      return ltpValue > open ? 'text-success' : ltpValue < open ? 'text-danger' : '';
    };
    
    // formatOI function is already defined above with enhanced functionality
    
    const getCellClass = (field, optionType, row) => {
      const option = row[optionType];
      if (!option) return '';
      
      const classes = [];
      
      // Add color based on change
      if (field === 'ltp' || field === 'change') {
        if (option.change > 0) classes.push('text-success');
        else if (option.change < 0) classes.push('text-danger');
      }
      
      // Add ATM class for strike price
      if (row.is_atm) classes.push('atm-cell');
      
      return classes.join(' ');
    };
    
    const formatLTP = (item, type) => {
      const ltp = item[`${type}_ltp`];
      const ohlc = item[`${type}_ohlc`];
      
      if (!ohlc) return ltp;
      
      const [open] = ohlc.split('|').map(val => parseFloat(val));
      const ltpValue = parseFloat(ltp);
      const oppositeType = type === 'ce' ? 'pe' : 'ce';
      const position = item[`${oppositeType}_position`];
      
      const diff = (ltpValue - open).toFixed(2);
      
      if (position) {
        return `${ltp}(${position.quantity})(${diff})`;
      }
      return `${ltp}(${diff})`;
    };
    
    const getRowClassName = (item) => {
      let classes = [];
      
      if (item.is_atm) {
        classes.push('table-info');
      }
      
      if (item.ce_position || item.pe_position) {
        classes.push('table-warning');
      }
      
      return classes.join(' ');
    };
    
    return {
      // State
      selectedSymbol,
      selectedExpiry,
      optionChainData,
      metaData,
      loading,
      error,
      lastFetched,
      
      // Computed
      availableSymbols,
      currentExpiries,
      
      // Methods
      formatExpiryDate,
      formatLastUpdated,
      formatStrike,
      getLtpColor,
      formatLTP,
      formatPrice,
      formatChange,
      getVolumeIntensity,
      formatOI,
      getCellClass,
      getRowClassName,
      calculateTotal,
      fetchOptionChain,
      formatNumber
    };
  }
};
</script>

<style scoped>
/* Clean, light theme */
.option-chain-container {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
  font-size: 13px;
  line-height: 1.2;
}

.option-chain-table {
  --table-bg: #ffffff;
  --table-header-bg: #f8f9fa;
  --table-border: #e9ecef;
  --text-color: #212529;
  --text-muted: #6c757d;
  --success-color: #28a745;
  --danger-color: #dc3545;
  --primary-color: #007bff;
  --secondary-color: #6c757d;
  --strike-bg: #f8f9fa;
  --hover-bg: rgba(0, 0, 0, 0.02);
  --atm-bg: #e7f5ff;
  
  background-color: var(--table-bg);
  border: 1px solid var(--table-border);
  border-radius: 6px;
  overflow: hidden;
  box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
}

.option-chain-table table {
  margin-bottom: 0;
  color: var(--text-color);
  border-collapse: separate;
  border-spacing: 0;
  width: 100%;
}

.option-chain-table thead th {
  background-color: var(--table-header-bg);
  color: var(--text-muted);
  font-weight: 500;
  text-transform: uppercase;
  font-size: 11px;
  letter-spacing: 0.5px;
  padding: 6px 8px;
  border: none;
  white-space: nowrap;
}

.option-chain-table tbody td {
  padding: 4px 8px;
  border: none;
  border-bottom: 1px solid var(--table-border);
  vertical-align: middle;
  transition: background-color 0.2s;
}

.option-chain-table tbody tr:last-child td {
  border-bottom: none;
}

.option-chain-table tbody tr:hover {
  background-color: var(--hover-bg) !important;
}

/* Compact cells */
.option-chain-table td, 
.option-chain-table th {
  padding: 4px 6px !important;
}

/* Text colors */
.text-success {
  color: var(--success-color) !important;
}

.text-danger {
  color: var(--danger-color) !important;
}

/* Strike price styling */
.strike-cell {
  background-color: var(--strike-bg) !important;
  font-weight: 600;
  color: var(--text-color) !important;
}

/* ATM row styling */
.atm-row {
  background-color: var(--atm-bg) !important;
}

.atm-row td {
  border-top: 1px solid var(--primary-color);
  border-bottom: 1px solid var(--primary-color);
}

.atm-row td:first-child {
  border-left: 1px solid var(--primary-color);
  border-top-left-radius: 4px;
  border-bottom-left-radius: 4px;
}

.atm-row td:last-child {
  border-right: 1px solid var(--primary-color);
  border-top-right-radius: 4px;
  border-bottom-right-radius: 4px;
}

/* Color intensity classes - light theme */
[class*="bg-oi-pe-"],
[class*="bg-oi-ce-"],
[class*="bg-volume-pe-"],
[class*="bg-volume-ce-"] {
  color: var(--text-color);
  font-weight: 500;
}

/* PUTS OI gradient - red */
.bg-oi-pe-1 { background-color: #ffebee; }
.bg-oi-pe-2 { background-color: #ffcdd2; }
.bg-oi-pe-3 { background-color: #ef9a9a; }
.bg-oi-pe-4 { background-color: #e57373; color: white; }
.bg-oi-pe-5 { background-color: #ef5350; color: white; }
.bg-oi-pe-6 { background-color: #f44336; color: white; }

/* CALLS OI gradient - green */
.bg-oi-ce-1 { background-color: #e8f5e9; }
.bg-oi-ce-2 { background-color: #c8e6c9; }
.bg-oi-ce-3 { background-color: #a5d6a7; }
.bg-oi-ce-4 { background-color: #81c784; color: white; }
.bg-oi-ce-5 { background-color: #66bb6a; color: white; }
.bg-oi-ce-6 { background-color: #4caf50; color: white; }

/* Volume gradients - more subtle */
.bg-volume-pe-1 { background-color: #fff3e0; }
.bg-volume-pe-2 { background-color: #ffe0b2; }
.bg-volume-pe-3 { background-color: #ffcc80; }
.bg-volume-pe-4 { background-color: #ffb74d; color: white; }

.bg-volume-ce-1 { background-color: #e3f2fd; }
.bg-volume-ce-2 { background-color: #bbdefb; }
.bg-volume-ce-3 { background-color: #90caf9; }
.bg-volume-ce-4 { background-color: #64b5f6; color: white; }

/* Responsive adjustments */
@media (max-width: 1400px) {
  .option-chain-container {
    font-size: 12px;
  }
  
  .option-chain-table th,
  .option-chain-table td {
    padding: 3px 4px !important;
  }
}

/* Custom scrollbar */
.option-chain-table::-webkit-scrollbar {
  height: 6px;
  width: 6px;
}

.option-chain-table::-webkit-scrollbar-track {
  background: var(--table-bg);
}

.option-chain-table::-webkit-scrollbar-thumb {
  background-color: var(--table-header-bg);
  border-radius: 3px;
}

/* Remove Bootstrap table styling */
.option-chain-table.table {
  --bs-table-bg: transparent;
  --bs-table-striped-bg: transparent;
  --bs-table-striped-color: var(--text-color);
  --bs-table-active-bg: var(--hover-bg);
  --bs-table-hover-bg: var(--hover-bg);
  --bs-table-hover-color: var(--text-color);
  border-color: var(--table-border);
}

.option-chain-table.table > :not(caption) > * > * {
  padding: 0.3rem 0.5rem;
  border-bottom-width: 1px;
  box-shadow: none;
}

.option-chain-table.table > thead > tr > th {
  border-bottom: 1px solid var(--table-border);
}

.option-chain-table.table > :not(:first-child) {
  border-top: none;
}

/* Custom classes for table cells */
.value-cell {
  font-family: 'Roboto Mono', monospace;
  letter-spacing: -0.3px;
}

/* Custom tooltip for more info */
[data-bs-toggle="tooltip"] {
  cursor: help;
  border-bottom: 1px dotted var(--text-muted);
}

/* Custom focus states */
.option-chain-table:focus-visible {
  outline: 2px solid var(--primary-color);
  outline-offset: 2px;
}
</style>

<style scoped>
/* Color intensity scale for OI/Volume */
/* CE (CALL) OI - Blue gradient */
.bg-oi-ce-1 { background-color: rgba(173, 216, 230, 0.2); } /* Light blue */
.bg-oi-ce-2 { background-color: rgba(100, 149, 237, 0.3); } /* Cornflower blue */
.bg-oi-ce-3 { background-color: rgba(65, 105, 225, 0.4); } /* Royal blue */
.bg-oi-ce-4 { background-color: rgba(0, 0, 255, 0.5); }   /* Blue */
.bg-oi-ce-5 { background-color: rgba(0, 0, 139, 0.6); }   /* Dark blue */
.bg-oi-ce-6 { background-color: rgba(0, 0, 128, 0.7); }   /* Navy */

/* PE (PUT) OI - Red gradient */
.bg-oi-pe-1 { background-color: rgba(255, 182, 193, 0.2); } /* Light pink */
.bg-oi-pe-2 { background-color: rgba(255, 160, 122, 0.3); } /* Light salmon */
.bg-oi-pe-3 { background-color: rgba(255, 99, 71, 0.4); }   /* Tomato */
.bg-oi-pe-4 { background-color: rgba(255, 0, 0, 0.5); }     /* Red */
.bg-oi-pe-5 { background-color: rgba(178, 34, 34, 0.6); }   /* Firebrick */
.bg-oi-pe-6 { background-color: rgba(139, 0, 0, 0.7); }     /* Dark red */

/* Volume - Green gradient for CALLs, Orange for PUTs */
.bg-volume-ce-1 { background-color: rgba(144, 238, 144, 0.2); } /* Light green */
.bg-volume-ce-2 { background-color: rgba(50, 205, 50, 0.3); }   /* Lime green */
.bg-volume-ce-3 { background-color: rgba(34, 139, 34, 0.4); }    /* Forest green */
.bg-volume-ce-4 { background-color: rgba(0, 100, 0, 0.5); }     /* Dark green */

.bg-volume-pe-1 { background-color: rgba(255, 218, 185, 0.2); } /* Peach puff */
.bg-volume-pe-2 { background-color: rgba(255, 165, 0, 0.3); }   /* Orange */
.bg-volume-pe-3 { background-color: rgba(255, 140, 0, 0.4); }   /* Dark orange */
.bg-volume-pe-4 { background-color: rgba(255, 69, 0, 0.5); }    /* Orange red */

/* Text colors for better contrast */
.bg-oi-ce-1, .bg-oi-ce-2, .bg-oi-pe-1, .bg-oi-pe-2,
.bg-volume-ce-1, .bg-volume-ce-2, .bg-volume-pe-1, .bg-volume-pe-2 {
  color: #000 !important;  /* Dark text for light backgrounds */
}

.bg-oi-ce-3, .bg-oi-ce-4, .bg-oi-ce-5, .bg-oi-ce-6,
.bg-oi-pe-3, .bg-oi-pe-4, .bg-oi-pe-5, .bg-oi-pe-6,
.bg-volume-ce-3, .bg-volume-ce-4, .bg-volume-pe-3, .bg-volume-pe-4 {
  color: #fff !important;  /* White text for dark backgrounds */
}

/* Table container */
.option-chain-table {
  position: relative;
  overflow: hidden;
}

/* ATM row styling */
.atm-row {
  background-color: #03a9f4 !important;
  color: white !important;
  font-weight: 600;
  position: relative;
  transition: background-color 0.2s ease;
}

.atm-row:hover {
  background-color: #0288d1 !important;
}

/* Ensure all text in the row is white for better contrast */
.atm-row,
.atm-row a,
.atm-row .text-success,
.atm-row .text-danger,
.atm-row .text-muted {
  color: white !important;
}

/* Make sure the strike price cell is also properly colored */
.atm-row .bg-light {
  background-color: rgba(255, 255, 255, 0.2) !important;
}

/* Full-width background for table rows */
.option-chain-table table {
  position: relative;
  z-index: 1;
}

.option-chain-table .table-responsive {
  position: relative;
  z-index: 1;
}

.option-chain-table .table {
  margin-bottom: 0;
}

/* Full-width background for ATM row */
.option-chain-table::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: #f8f9fa; /* Default table background */
  z-index: 0;
  pointer-events: none;
}

.option-chain-table .atm-row td {
  position: relative;
  z-index: 2;
  background-color: transparent !important;
}

.option-chain-table-container {
  overflow-x: auto;
  margin: 1rem 0;
  border-radius: 8px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.option-chain-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.875rem;
  background: white;
}

.option-chain-table th,
.option-chain-table td {
  padding: 0.5rem;
  text-align: center;
  border: 1px solid #e2e8f0;
}

.option-chain-table th {
  background-color: #f8fafc;
  font-weight: 600;
  color: #475569;
  white-space: nowrap;
}

.option-chain-table tbody tr:hover {
  background-color: #f8fafc;
}

/* ATM row styling */
.option-chain-table .atm-row {
  background-color: #f0f9ff;
  font-weight: 600;
}

.option-chain-table .atm-row:hover {
  background-color: #e0f2fe;
}

/* Strike price column */
.option-chain-table .strike-col {
  background-color: #f8fafc;
  font-weight: 600;
  color: #1e40af;
}

/* Text colors */
.text-success {
  color: #16a34a;
}

.text-danger {
  color: #dc2626;
}

/* Responsive adjustments */
@media (max-width: 1024px) {
  .option-chain-table {
    font-size: 0.8125rem;
  }
  
  .option-chain-table th,
  .option-chain-table td {
    padding: 0.375rem 0.25rem;
  }
}

@media (max-width: 768px) {
  .option-chain-table {
    font-size: 0.75rem;
  }
  
  .option-chain-table th,
  .option-chain-table td {
    padding: 0.25rem 0.125rem;
  }
}

.option-chain-container {
  padding: 1rem;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
}

.compact-controls {
  display: flex;
  flex-wrap: wrap;
  gap: 1rem;
  align-items: center;
  margin-bottom: 1rem;
  padding: 0.5rem 0;
}

.control-group {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.filter-group {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.compact-select, .form-select {
  padding: 0.25rem 0.5rem;
  font-size: 0.875rem;
  height: 31px;
  border: 1px solid #ced4da;
  border-radius: 0.25rem;
  background-color: white;
  color: #212529;
  cursor: pointer;
  min-width: 120px;
}

.compact-select:disabled, .form-select:disabled {
  opacity: 0.7;
  cursor: not-allowed;
  background-color: #e9ecef;
}

.compact-select option, .form-select option {
  background-color: white;
  color: #212529;
}

.compact-select:focus, .form-select:focus {
  outline: none;
  border-color: #86b7fe;
  box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.25);
}

/* Refresh button styles */
.refresh-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 0.5rem;
  padding: 0.25rem 0.75rem;
  font-size: 0.875rem;
  min-width: 100px;
  height: 31px;
}

/* Last updated text */
.last-updated {
  margin-left: auto;
  font-size: 0.8125rem;
  color: #6c757d;
  font-style: italic;
}

/* Responsive adjustments */
@media (max-width: 768px) {
  .compact-controls {
    flex-direction: column;
    align-items: flex-start;
    gap: 0.75rem;
  }
  
  .last-updated {
    margin-left: 0;
    margin-top: 0.5rem;
  }
  
  .filter-group {
    width: 100%;
  }
  
  .compact-select, .form-select {
    flex: 1;
  }
}

label {
  font-weight: 500;
  font-size: 0.875rem;
  margin-bottom: 0;
  white-space: nowrap;
}

select {
  min-width: 120px;
}

.meta-data {
  display: flex;
  flex-wrap: wrap;
  gap: 1rem;
  margin-bottom: 1rem;
  padding: 0.75rem;
  background-color: #f8f9fa;
  border-radius: 4px;
  font-size: 0.9rem;
}

.meta-item {
  margin-right: 1rem;
}

.loading, .error {
  padding: 2rem;
  text-align: center;
  background-color: #f5f5f5;
  border-radius: 4px;
  margin-top: 1rem;
}

.error {
  color: #ff3860;
  background-color: #feecf0;
}

.option-chain-table-container {
  overflow-x: auto;
}

.option-chain-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.9rem;
  margin-top: 1rem;
}

.option-chain-table th,
.option-chain-table td {
  padding: 0.5rem;
  text-align: center;
  border: 1px solid #dee2e6;
}

.option-chain-table thead th {
  background-color: #f8f9fa;
  position: sticky;
  top: 0;
  z-index: 1;
  font-weight: 600;
}

/* Table row styling based on the React reference */
.table-info {
  background-color: rgba(209, 236, 241, 0.3) !important;
}

.table-warning {
  background-color: rgba(255, 243, 205, 0.3) !important;
}

/* Make the table more compact for better readability */
.option-chain-table.table-sm td,
.option-chain-table.table-sm th {
  padding: 0.3rem;
}

/* Add some hover effect for better UX */
.option-chain-table tbody tr:hover {
  background-color: rgba(0, 0, 0, 0.02);
}
</style>
