import React, { useState, useEffect } from 'react';
import { 
  Table, 
  Card, 
  Input, 
  Button, 
  Space, 
  Tag, 
  Typography, 
  Rate, 
  Image,
  Row,
  Col,
  Modal,
  Spin
} from 'antd';
import { SearchOutlined, VideoCameraOutlined } from '@ant-design/icons';
import { useMovies } from '@/hooks/useMovieData';
import type { Movie } from '@/models/movie.model';
import { useNavigate } from 'react-router-dom';

const { Title, Text } = Typography;

/**
 * Movie List Page Component
 * Displays a paginated list of movies with search and filter capabilities
 */
export const MovieList: React.FC = () => {
  const [searchText, setSearchText] = useState('');
  const [filteredMovies, setFilteredMovies] = useState<Movie[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedMovie, setSelectedMovie] = useState<Movie | null>(null);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const navigate = useNavigate();

  const { movies, loading: hookLoading } = useMovies(1, 50); // 获取50条数据用于列表展示

  useEffect(() => {
    if (movies) {
      setFilteredMovies(movies);
      setLoading(hookLoading);
    }
  }, [movies, hookLoading]);

  // 处理搜索
  const handleSearch = (value: string) => {
    setSearchText(value);
    if (!value) {
      setFilteredMovies(movies || []);
      return;
    }

    const filtered = (movies || []).filter(movie => 
      movie.title.toLowerCase().includes(value.toLowerCase()) ||
      movie.directors.some(director => director.toLowerCase().includes(value.toLowerCase())) ||
      movie.cast.some(actor => actor.toLowerCase().includes(value.toLowerCase())) ||
      movie.genres.some(genre => genre.toLowerCase().includes(value.toLowerCase()))
    );

    setFilteredMovies(filtered);
  };

  // 显示电影详情
  const showMovieDetail = (movie: Movie) => {
    setSelectedMovie(movie);
    setIsModalVisible(true);
  };

  // 跳转到电影详情页面
  const goToMovieDetail = (movieId: string) => {
    navigate(`/moviedetail/${movieId}`);
  };

  // 列表列定义
  const columns = [
    {
      title: '海报',
      dataIndex: 'poster',
      key: 'poster',
      render: (poster: string, record: Movie) => (
        <Image
          src={poster || 'https://via.placeholder.com/60x80?text=海报'}
          alt={record.title}
          width={60}
          height={80}
          style={{ objectFit: 'cover' }}
          preview={false}
        />
      ),
    },
    {
      title: '电影名称',
      dataIndex: 'title',
      key: 'title',
      sorter: (a: Movie, b: Movie) => a.title.localeCompare(b.title),
      render: (title: string, record: Movie) => (
        <a onClick={() => goToMovieDetail(record.id)} style={{ fontWeight: 'bold' }}>
          {title}
        </a>
      ),
    },
    {
      title: '年份',
      dataIndex: 'year',
      key: 'year',
      width: 100,
      sorter: (a: Movie, b: Movie) => a.year - b.year,
    },
    {
      title: '评分',
      key: 'rating',
      width: 120,
      sorter: (a: Movie, b: Movie) => a.rating - b.rating,
      render: (_: any, record: Movie) => (
        <Space>
          <Rate disabled allowHalf value={record.rating / 2} />
          <Text strong>{record.rating.toFixed(1)}</Text>
        </Space>
      ),
    },
    {
      title: '类型',
      dataIndex: 'genres',
      key: 'genres',
      render: (genres: string[]) => (
        <Space wrap>
          {genres?.map((genre, index) => (
            <Tag key={index} color="blue">
              {genre}
            </Tag>
          ))}
        </Space>
      ),
    },
    {
      title: '导演',
      dataIndex: 'directors',
      key: 'directors',
      render: (directors: string[]) => (
        <Text>{directors?.join(', ')}</Text>
      ),
    },
    {
      title: '主演',
      dataIndex: 'cast',
      key: 'cast',
      render: (cast: string[]) => (
        <div style={{ maxHeight: 60, overflow: 'hidden' }}>
          <Text type="secondary">{cast?.slice(0, 3).join(', ')}</Text>
          {cast && cast.length > 3 && <Text type="secondary"> 等{cast.length - 3}人</Text>}
        </div>
      ),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_: any, record: Movie) => (
        <Space>
          <Button 
            type="link" 
            onClick={() => goToMovieDetail(record.id)}
          >
            详情
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <div style={{ padding: 24, background: '#f0f2f5', minHeight: '100vh' }}>
      <Card 
        title={
          <Row justify="space-between" align="middle">
            <Col>
              <Title level={3} style={{ margin: 0 }}>
                <VideoCameraOutlined /> 电影数据列表
              </Title>
            </Col>
            <Col>
              <Space>
                <Input
                  placeholder="搜索电影名称、导演、演员或类型..."
                  prefix={<SearchOutlined />}
                  value={searchText}
                  onChange={e => handleSearch(e.target.value)}
                  style={{ width: 300 }}
                  allowClear
                />
              </Space>
            </Col>
          </Row>
        }
        style={{ borderRadius: 8 }}
      >
        {loading && (!movies || movies.length === 0) ? (
          <div
            style={{
              display: 'flex',
              justifyContent: 'center',
              alignItems: 'center',
              height: '200px',
            }}
          >
            <Spin size="large" tip="加载中..." />
          </div>
        ) : (
          <Table
            columns={columns}
            dataSource={filteredMovies}
            rowKey="id"
            pagination={{
              pageSize: 10,
              showSizeChanger: true,
              showQuickJumper: true,
              showTotal: (total) => `共 ${total} 部电影`,
              pageSizeOptions: ['10', '20', '50', '100'],
            }}
            scroll={{ x: 1200 }}
          />
        )}
      </Card>

      {/* 电影详情模态框 */}
      <Modal
        title={selectedMovie?.title}
        open={isModalVisible}
        onCancel={() => setIsModalVisible(false)}
        footer={null}
        width={800}
      >
        {selectedMovie && (
          <div style={{ padding: '20px 0' }}>
            <Row gutter={24}>
              <Col span={8}>
                <Image
                  src={selectedMovie.poster || 'https://via.placeholder.com/200x300?text=海报'}
                  alt={selectedMovie.title}
                  width="100%"
                  height={300}
                  style={{ objectFit: 'cover', borderRadius: 8 }}
                />
              </Col>
              <Col span={16}>
                <div style={{ marginBottom: 16 }}>
                  <Title level={4}>评分</Title>
                  <div>
                    <Rate disabled allowHalf value={selectedMovie.rating / 2} />
                    <Text strong style={{ marginLeft: 8, fontSize: 18 }}>
                      {selectedMovie.rating.toFixed(1)}
                    </Text>
                  </div>
                </div>

                <div style={{ marginBottom: 16 }}>
                  <Title level={4}>年份</Title>
                  <Text>{selectedMovie.year}</Text>
                </div>

                <div style={{ marginBottom: 16 }}>
                  <Title level={4}>类型</Title>
                  <Space wrap>
                    {selectedMovie.genres.map((genre, index) => (
                      <Tag key={index} color="blue">
                        {genre}
                      </Tag>
                    ))}
                  </Space>
                </div>

                <div style={{ marginBottom: 16 }}>
                  <Title level={4}>导演</Title>
                  <Text>{selectedMovie.directors.join(', ')}</Text>
                </div>

                <div style={{ marginBottom: 16 }}>
                  <Title level={4}>主演</Title>
                  <Text>{selectedMovie.cast.join(', ')}</Text>
                </div>
              </Col>
            </Row>

            <div style={{ marginTop: 24 }}>
              <Title level={4}>剧情简介</Title>
              <Text type="secondary">
                {selectedMovie.summary || '暂无简介信息'}
              </Text>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};