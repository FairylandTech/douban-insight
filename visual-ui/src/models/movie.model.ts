/**
 * Movie data model
 */
export interface Movie {
  id: string;
  title: string;
  rating: number;
  year: number;
  directors: string[];
  cast: string[];
  genres: string[];
  poster?: string;
  summary?: string;
}

/**
 * Comment data model
 */
export interface Comment {
  id: string;
  movieId: string;
  author: string;
  content: string;
  rating: number;
  createdAt: string;
  sentiment?: 'positive' | 'negative' | 'neutral';
  sentimentScore?: number;
}

/**
 * Rating distribution data
 */
export interface RatingDistribution {
  rating: number;
  count: number;
  percentage: number;
}

/**
 * Time series data for trends
 */
export interface TimeSeriesData {
  date: string;
  count: number;
  avgRating?: number;
}

/**
 * Dashboard statistics
 */
export interface DashboardStats {
  totalMovies: number;
  totalComments: number;
  avgRating: number;
  ratingDistribution: RatingDistribution[];
  commentTrends: TimeSeriesData[];
  topMovies: Movie[];
  sentimentStats: {
    positive: number;
    negative: number;
    neutral: number;
  };
}
