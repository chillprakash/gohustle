<template>
  <div class="login-container">
    <div class="login-card">
      <h2>Login to GoHustle</h2>
      <form @submit.prevent="handleLogin" class="login-form">
        <div class="form-group">
          <label for="username">Username</label>
          <input 
            type="text" 
            id="username" 
            v-model="credentials.username" 
            required 
            placeholder="Enter your username"
          />
        </div>
        
        <div class="form-group">
          <label for="password">Password</label>
          <input 
            type="password" 
            id="password" 
            v-model="credentials.password" 
            required 
            placeholder="Enter your password"
          />
        </div>
        
        <div v-if="error" class="error-message">
          {{ error }}
        </div>
        
        <button type="submit" :disabled="loading">
          {{ loading ? 'Logging in...' : 'Login' }}
        </button>
      </form>
    </div>
  </div>
</template>

<script>
import { ref, computed } from 'vue';
import { useStore } from 'vuex';
import { useRouter } from 'vue-router/dist/vue-router.esm-bundler';

export default {
  name: 'LoginView',
  setup() {
    const store = useStore();
    const router = useRouter();
    
    const credentials = ref({
      username: '',
      password: ''
    });
    
    const error = computed(() => store.getters.error);
    const loading = computed(() => store.getters.isLoading);
    
    const handleLogin = async () => {
      try {
        const response = await store.dispatch('login', credentials.value);
        if (response && response.success) {
          // Store the token in localStorage
          localStorage.setItem('auth_token', response.data.token);
          // Redirect to option chain page
          router.push('/optionchain');
        }
      } catch (err) {
        // Error is handled in the store action
        console.error('Login failed:', err);
        error.value = err.message || 'Login failed. Please check your credentials.';
      }
    };
    
    return {
      credentials,
      error,
      loading,
      handleLogin
    };
  }
};
</script>

<style scoped>
.login-container {
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 80vh;
}

.login-card {
  background-color: #fff;
  border-radius: 8px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
  padding: 2rem;
  width: 100%;
  max-width: 400px;
}

h2 {
  color: #3273dc;
  margin-bottom: 1.5rem;
  text-align: center;
}

.login-form {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

label {
  font-weight: 500;
  color: #4a4a4a;
}

input {
  padding: 0.75rem;
  border: 1px solid #dbdbdb;
  border-radius: 4px;
  font-size: 1rem;
}

input:focus {
  border-color: #3273dc;
  outline: none;
  box-shadow: 0 0 0 2px rgba(50, 115, 220, 0.25);
}

button {
  background-color: #3273dc;
  color: white;
  border: none;
  padding: 0.75rem;
  border-radius: 4px;
  font-size: 1rem;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.2s ease;
}

button:hover {
  background-color: #2366d1;
}

button:disabled {
  background-color: #b5ccf2;
  cursor: not-allowed;
}

.error-message {
  color: #ff3860;
  font-size: 0.875rem;
  padding: 0.5rem;
  background-color: #feecf0;
  border-radius: 4px;
}
</style>
