import { http, HttpResponse } from 'msw';
import type { Response } from '@/models/response.model';
import { mockMovies, mockComments, mockDashboardStats } from './data/movies';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8080/api';

/**
 * MSW request handlers for mocking API responses
 */
export const handlers = [
  // Get dashboard statistics
  http.get(`${API_BASE_URL}/dashboard/stats`, () => {
    const response: Response<typeof mockDashboardStats> = {
      code: 200,
      message: 'Success',
      data: mockDashboardStats,
    };
    return HttpResponse.json(response);
  }),

  // Get all movies
  http.get(`${API_BASE_URL}/movies`, ({ request }) => {
    const url = new URL(request.url);
    const page = parseInt(url.searchParams.get('page') || '1');
    const pageSize = parseInt(url.searchParams.get('pageSize') || '10');

    const start = (page - 1) * pageSize;
    const end = start + pageSize;
    const paginatedMovies = mockMovies.slice(start, end);

    const response: Response<typeof paginatedMovies> = {
      code: 200,
      message: 'Success',
      data: paginatedMovies,
    };
    return HttpResponse.json(response);
  }),

  // Get movie by ID
  http.get(`${API_BASE_URL}/movies/:id`, ({ params }) => {
    const { id } = params;
    const movie = mockMovies.find((m) => m.id === id);

    if (!movie) {
      const errorResponse: Response<null> = {
        code: 404,
        message: 'Movie not found',
        data: null,
      };
      return HttpResponse.json(errorResponse, { status: 404 });
    }

    const response: Response<typeof movie> = {
      code: 200,
      message: 'Success',
      data: movie,
    };
    return HttpResponse.json(response);
  }),

  // Get movie comments
  http.get(`${API_BASE_URL}/movies/:movieId/comments`, ({ params }) => {
    const { movieId } = params;
    const comments = mockComments.filter((c) => c.movieId === movieId);

    const response: Response<typeof comments> = {
      code: 200,
      message: 'Success',
      data: comments,
    };
    return HttpResponse.json(response);
  }),

  // Get rating distribution
  http.get(`${API_BASE_URL}/stats/rating-distribution`, () => {
    const response: Response<typeof mockDashboardStats.ratingDistribution> = {
      code: 200,
      message: 'Success',
      data: mockDashboardStats.ratingDistribution,
    };
    return HttpResponse.json(response);
  }),

  // Get comment trends
  http.get(`${API_BASE_URL}/stats/comment-trends`, () => {
    const response: Response<typeof mockDashboardStats.commentTrends> = {
      code: 200,
      message: 'Success',
      data: mockDashboardStats.commentTrends,
    };
    return HttpResponse.json(response);
  }),

  // Get top movies
  http.get(`${API_BASE_URL}/stats/top-movies`, ({ request }) => {
    const url = new URL(request.url);
    const limit = parseInt(url.searchParams.get('limit') || '10');
    const topMovies = mockMovies.slice(0, limit);

    const response: Response<typeof topMovies> = {
      code: 200,
      message: 'Success',
      data: topMovies,
    };
    return HttpResponse.json(response);
  }),
];
