import { create } from 'zustand';
import type { DashboardStats, Movie } from '@/models/movie.model';

/**
 * Movie store state interface
 */
interface MovieStoreState {
  // State
  dashboardStats: DashboardStats | null;
  movies: Movie[];
  selectedMovie: Movie | null;
  loading: boolean;
  error: string | null;

  // Actions
  setDashboardStats: (stats: DashboardStats | null) => void;
  setMovies: (movies: Movie[]) => void;
  setSelectedMovie: (movie: Movie | null) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  reset: () => void;
}

/**
 * Initial state
 */
const initialState = {
  dashboardStats: null,
  movies: [],
  selectedMovie: null,
  loading: false,
  error: null,
};

/**
 * Movie store using Zustand
 */
export const useMovieStore = create<MovieStoreState>((set) => ({
  ...initialState,

  setDashboardStats: (stats) => set({ dashboardStats: stats }),

  setMovies: (movies) => set({ movies }),

  setSelectedMovie: (movie) => set({ selectedMovie: movie }),

  setLoading: (loading) => set({ loading }),

  setError: (error) => set({ error }),

  reset: () => set(initialState),
}));
