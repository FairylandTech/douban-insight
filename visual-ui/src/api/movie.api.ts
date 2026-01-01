import { get } from './client';
import type { Response } from '@/models/response.model';
import type {
  Movie,
  Comment,
  DashboardStats,
  RatingDistribution,
  TimeSeriesData,
} from '@/models/movie.model';

/**
 * Movie API endpoints
 */
export const movieApi = {
  /**
   * Get dashboard statistics
   */
  getDashboardStats: (): Promise<Response<DashboardStats>> => {
    return get<DashboardStats>('/dashboard/stats');
  },

  /**
   * Get all movies
   */
  getMovies: (params?: { page?: number; pageSize?: number }): Promise<Response<Movie[]>> => {
    return get<Movie[]>('/movies', { params });
  },

  /**
   * Get movie by ID
   */
  getMovieById: (id: string): Promise<Response<Movie>> => {
    return get<Movie>(`/movies/${id}`);
  },

  /**
   * Get comments for a movie
   */
  getMovieComments: (
    movieId: string,
    params?: { page?: number; pageSize?: number }
  ): Promise<Response<Comment[]>> => {
    return get<Comment[]>(`/movies/${movieId}/comments`, { params });
  },

  /**
   * Get rating distribution
   */
  getRatingDistribution: (): Promise<Response<RatingDistribution[]>> => {
    return get<RatingDistribution[]>('/stats/rating-distribution');
  },

  /**
   * Get comment trends
   */
  getCommentTrends: (params?: {
    startDate?: string;
    endDate?: string;
  }): Promise<Response<TimeSeriesData[]>> => {
    return get<TimeSeriesData[]>('/stats/comment-trends', { params });
  },

  /**
   * Get top rated movies
   */
  getTopMovies: (limit: number = 10): Promise<Response<Movie[]>> => {
    return get<Movie[]>('/stats/top-movies', { params: { limit } });
  },
};
