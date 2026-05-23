import axios from 'axios';

// Create an Axios instance
const apiClient = axios.create({
  baseURL: '/api', // Proxied via Vite to http://localhost:3000
  headers: {
    'Content-Type': 'application/json',
    'X-User-Role': 'ADMIN' // Simulating Admin login based on the Backend API requirement
  }
});

// Response interceptor to unwrap ApiResponse
apiClient.interceptors.response.use(
  (response) => {
    return response.data; // The Spring Boot ApiResponse<T>
  },
  (error) => {
    console.error("API Error:", error.response?.data || error.message);
    return Promise.reject(error.response?.data || { message: "Network Error" });
  }
);

export const fetchPosts = () => apiClient.get('/posts');
export const fetchComments = (postId) => apiClient.get(`/comments?postId=${postId}`);
export const createPost = (data) => apiClient.post('/posts', data);

// Media upload needs multipart/form-data
export const uploadMedia = async (file) => {
  const formData = new FormData();
  formData.append('file', file);
  return apiClient.post('/media/uploads', formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    }
  });
};

export default apiClient;
