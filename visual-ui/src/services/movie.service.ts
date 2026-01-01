import { movieApi } from '@/api/movie.api';
import type {
  Movie,
  Comment,
  DashboardStats,
  RatingDistribution,
  TimeSeriesData,
} from '@/models/movie.model';

/**
 * Movie service layer - provides business logic on top of API calls
 */
export class MovieService {
  /**
   * Get dashboard statistics with error handling
   */
  static async getDashboardStats(): Promise<DashboardStats | null> {
    try {
      const response = await movieApi.getDashboardStats();
      if (response.code === 200) {
        return response.data;
      }
      console.error('Failed to fetch dashboard stats:', response.message);
      return null;
    } catch (error) {
      console.error('Error fetching dashboard stats:', error);
      return null;
    }
  }

  /**
   * Get movies with pagination
   */
  static async getMovies(page: number = 1, pageSize: number = 10): Promise<Movie[]> {
    try {
      const response = await movieApi.getMovies({ page, pageSize });
      if (response.code === 200) {
        return response.data;
      }
      console.error('Failed to fetch movies:', response.message);
      return [];
    } catch (error) {
      console.error('Error fetching movies:', error);
      return [];
    }
  }

  /**
   * Get movie by ID
   */
  static async getMovieById(id: string): Promise<Movie | null> {
    try {
      const response = await movieApi.getMovieById(id);
      if (response.code === 200) {
        return response.data;
      }
      console.error('Failed to fetch movie:', response.message);
      return null;
    } catch (error) {
      console.error('Error fetching movie:', error);
      return null;
    }
  }

  /**
   * Get comments for a movie
   */
  static async getMovieComments(
    movieId: string,
    page: number = 1,
    pageSize: number = 20
  ): Promise<Comment[]> {
    try {
      const response = await movieApi.getMovieComments(movieId, { page, pageSize });
      if (response.code === 200) {
        return response.data;
      }
      console.error('Failed to fetch comments:', response.message);
      return [];
    } catch (error) {
      console.error('Error fetching comments:', error);
      return [];
    }
  }

  /**
   * Get rating distribution statistics
   */
  static async getRatingDistribution(): Promise<RatingDistribution[]> {
    try {
      const response = await movieApi.getRatingDistribution();
      if (response.code === 200) {
        return response.data;
      }
      console.error('Failed to fetch rating distribution:', response.message);
      return [];
    } catch (error) {
      console.error('Error fetching rating distribution:', error);
      return [];
    }
  }

  /**
   * Get comment trends over time
   */
  static async getCommentTrends(
    startDate?: string,
    endDate?: string
  ): Promise<TimeSeriesData[]> {
    try {
      const response = await movieApi.getCommentTrends({ startDate, endDate });
      if (response.code === 200) {
        return response.data;
      }
      console.error('Failed to fetch comment trends:', response.message);
      return [];
    } catch (error) {
      console.error('Error fetching comment trends:', error);
      return [];
    }
  }

  /**
   * Get top rated movies
   */
  static async getTopMovies(limit: number = 10): Promise<Movie[]> {
    try {
      const response = await movieApi.getTopMovies(limit);
      if (response.code === 200) {
        return response.data;
      }
      console.error('Failed to fetch top movies:', response.message);
      return [];
    } catch (error) {
      console.error('Error fetching top movies:', error);
      return [];
    }
  }
}
