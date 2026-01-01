import React from 'react';
import { Card, List, Tag, Rate } from 'antd';
import type { Movie } from '@/models/movie.model';

interface TopMoviesListProps {
  movies: Movie[];
  loading?: boolean;
}

/**
 * Top Movies List Component
 * Displays a ranked list of top-rated movies
 */
export const TopMoviesList: React.FC<TopMoviesListProps> = ({ movies, loading = false }) => {
  return (
    <Card title="热门电影 Top 10" loading={loading} style={{ height: '100%' }}>
      <List
        itemLayout="horizontal"
        dataSource={movies}
        renderItem={(movie, index) => (
          <List.Item>
            <List.Item.Meta
              avatar={
                <div
                  style={{
                    width: 40,
                    height: 40,
                    borderRadius: '50%',
                    background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    color: 'white',
                    fontWeight: 'bold',
                    fontSize: 18,
                  }}
                >
                  {index + 1}
                </div>
              }
              title={
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ fontWeight: 500 }}>{movie.title}</span>
                  <Tag color="blue">{movie.year}</Tag>
                </div>
              }
              description={
                <div>
                  <Rate disabled defaultValue={movie.rating / 2} style={{ fontSize: 14 }} />
                  <span style={{ marginLeft: 8, color: '#faad14', fontWeight: 'bold' }}>
                    {movie.rating}
                  </span>
                  <div style={{ marginTop: 4 }}>
                    {movie.genres.map((genre) => (
                      <Tag key={genre} style={{ marginRight: 4 }}>
                        {genre}
                      </Tag>
                    ))}
                  </div>
                </div>
              }
            />
          </List.Item>
        )}
      />
    </Card>
  );
};
