import { createRouter, createWebHistory } from 'vue-router/dist/vue-router.esm-bundler'
import store from '../store'

const routes = [
  {
    path: '/',
    redirect: '/optionchain'
  },
  {
    path: '/login',
    name: 'LoginView',
    component: () => import('../views/Login.vue'),
    meta: { requiresAuth: false }
  },
  {
    path: '/optionchain',
    name: 'OptionChainView',
    component: () => import('../views/OptionChain.vue'),
    meta: { requiresAuth: true }
  }
]

const router = createRouter({
  history: createWebHistory(process.env.BASE_URL),
  routes
})

router.beforeEach((to, from, next) => {
  const requiresAuth = to.matched.some(record => record.meta.requiresAuth)
  const isAuthenticated = store.getters.isAuthenticated
  
  if (requiresAuth && !isAuthenticated) {
    next({ name: 'LoginView', query: { redirect: to.fullPath } })
  } else if (to.path === '/login' && isAuthenticated) {
    next({ name: 'OptionChainView' })
  } else {
    next()
  }
})

export default router
