import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import store from './store'
import 'bootstrap/dist/css/bootstrap.min.css'
import 'bootstrap/dist/js/bootstrap.bundle.min.js'
import './assets/styles/main.css'

const app = createApp(App)

// Set up global error handler
app.config.errorHandler = (err, vm, info) => {
  console.error('Vue error:', err)
  console.error('Error info:', info)
  // You can also show a user-friendly error message here
}

app.use(store)
app.use(router)

// Global properties
app.config.globalProperties.$filters = {
  formatDate(value) {
    if (!value) return ''
    return new Date(value).toLocaleDateString()
  },
  formatCurrency(value) {
    if (value === undefined || value === null) return '₹0.00'
    return `₹${parseFloat(value).toFixed(2).replace(/\d(?=(\d{3})+\.)/g, '$&,')}`
  }
}

app.mount('#app')

// Log app initialization
console.log('App initialized in', process.env.NODE_ENV, 'mode')
