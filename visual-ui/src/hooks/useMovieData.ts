import { useEffect, useState } from 'react';
import { MovieService } from '@/services/movie.service';
import { useMovieStore } from '@/stores/movieStore';

/**
 * Custom hook for fetching and managing dashboard data
 */
export const useDashboardData = () => {
  const { dashboardStats, setDashboardStats, setLoading, setError } = useMovieStore();
  const [isInitialized, setIsInitialized] = useState(false);

  useEffect(() => {
    const fetchDashboardStats = async () => {
      if (isInitialized) return;

      setLoading(true);
      setError(null);

      try {
        const stats = await MovieService.getDashboardStats();
        if (stats) {
          setDashboardStats(stats);
        } else {
          setError('Failed to load dashboard statistics');
        }
      } catch (error) {
        setError('An error occurred while loading data');
        console.error('Dashboard fetch error:', error);
      } finally {
        setLoading(false);
        setIsInitialized(true);
      }
    };

    fetchDashboardStats();
  }, [isInitialized, setDashboardStats, setLoading, setError]);

  return {
    dashboardStats,
    loading: useMovieStore((state) => state.loading),
    error: useMovieStore((state) => state.error),
    refetch: () => setIsInitialized(false),
  };
};

/**
 * Custom hook for fetching movies list
 */
export const useMovies = (page: number = 1, pageSize: number = 10) => {
  const { movies, setMovies, setLoading, setError } = useMovieStore();
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    const fetchMovies = async () => {
      setIsLoading(true);
      setLoading(true);
      setError(null);

      try {
        const data = await MovieService.getMovies(page, pageSize);
        setMovies(data);
      } catch (error) {
        setError('Failed to load movies');
        console.error('Movies fetch error:', error);
      } finally {
        setIsLoading(false);
        setLoading(false);
      }
    };

    fetchMovies();
  }, [page, pageSize, setMovies, setLoading, setError]);

  return {
    movies,
    loading: isLoading,
    error: useMovieStore((state) => state.error),
  };
};

/**
 * Custom hook for fetching a single movie
 */
export const useMovie = (movieId: string | null) => {
  const { selectedMovie, setSelectedMovie, setLoading, setError } = useMovieStore();
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!movieId) {
      setSelectedMovie(null);
      return;
    }

    const fetchMovie = async () => {
      setIsLoading(true);
      setLoading(true);
      setError(null);

      try {
        const data = await MovieService.getMovieById(movieId);
        setSelectedMovie(data);
      } catch (error) {
        setError('Failed to load movie details');
        console.error('Movie fetch error:', error);
      } finally {
        setIsLoading(false);
        setLoading(false);
      }
    };

    fetchMovie();
  }, [movieId, setSelectedMovie, setLoading, setError]);

  return {
    movie: selectedMovie,
    loading: isLoading,
    error: useMovieStore((state) => state.error),
  };
};
