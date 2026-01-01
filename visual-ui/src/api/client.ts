import axios from 'axios';
import type { AxiosInstance, AxiosRequestConfig, AxiosResponse, AxiosError } from 'axios';
import type { Response } from '@/models/response.model';

/**
 * API Client configuration
 */
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8080/api';
const TIMEOUT = 30000;

/**
 * Create axios instance with default configuration
 */
const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  timeout: TIMEOUT,
  headers: {
    'Content-Type': 'application/json',
  },
});

/**
 * Request interceptor
 */
apiClient.interceptors.request.use(
  (config) => {
    // Add auth token if exists
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error: AxiosError) => {
    console.error('Request error:', error);
    return Promise.reject(error);
  }
);

/**
 * Response interceptor
 */
apiClient.interceptors.response.use(
  (response: AxiosResponse<Response<unknown>>) => {
    // Return response data directly
    return response;
  },
  (error: AxiosError<Response<unknown>>) => {
    // Handle error responses
    if (error.response) {
      const { code, message } = error.response.data || {};
      console.error(`API Error [${code}]:`, message);

      // Handle specific error codes
      switch (code) {
        case 401:
          // Unauthorized - redirect to login
          localStorage.removeItem('token');
          window.location.href = '/login';
          break;
        case 403:
          console.error('Forbidden: You do not have permission to access this resource');
          break;
        case 404:
          console.error('Resource not found');
          break;
        case 500:
          console.error('Server error');
          break;
      }
    } else if (error.request) {
      console.error('No response received:', error.request);
    } else {
      console.error('Request setup error:', error.message);
    }
    return Promise.reject(error);
  }
);

/**
 * Generic GET request
 */
export const get = <T>(url: string, config?: AxiosRequestConfig): Promise<Response<T>> => {
  return apiClient.get<Response<T>>(url, config).then((res) => res.data);
};

/**
 * Generic POST request
 */
export const post = <T>(
  url: string,
  data?: unknown,
  config?: AxiosRequestConfig
): Promise<Response<T>> => {
  return apiClient.post<Response<T>>(url, data, config).then((res) => res.data);
};

/**
 * Generic PUT request
 */
export const put = <T>(
  url: string,
  data?: unknown,
  config?: AxiosRequestConfig
): Promise<Response<T>> => {
  return apiClient.put<Response<T>>(url, data, config).then((res) => res.data);
};

/**
 * Generic DELETE request
 */
export const del = <T>(url: string, config?: AxiosRequestConfig): Promise<Response<T>> => {
  return apiClient.delete<Response<T>>(url, config).then((res) => res.data);
};

export default apiClient;
