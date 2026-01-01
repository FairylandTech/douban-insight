import React from 'react';
import { Row, Col, Typography, Card, Tag, Space, Descriptions, Divider, Table, Spin } from 'antd';
import { 
    CalendarOutlined, 
    StarOutlined, 
    EyeOutlined, 
    TeamOutlined,
    FieldStringOutlined,
    EnvironmentOutlined
} from '@ant-design/icons';
import { useParams } from 'react-router-dom';
import { useMovie } from '@/hooks/useMovieData';

const { Title, Text } = Typography;

/**
 * Movie Detail Page Component
 * Displays detailed information about a specific movie
 */
export const MovieDetail: React.FC = () => {
    const { id } = useParams<{ id: string }>();
    const { movie: movieDetails, loading, error } = useMovie(id || null);

    if (error) {
        return (
            <div style={{ padding: 24 }}>
                <Text type="danger">加载电影详情失败: {error}</Text>
            </div>
        );
    }

    if (loading || !movieDetails) {
        return (
            <div
                style={{
                    display: 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                    height: '100vh',
                }}
            >
                <Spin size="large" tip="加载中..." />
            </div>
        );
    }

    // Mock data for cast and crew table
    const castColumns = [
        {
            title: '演员',
            dataIndex: 'name',
            key: 'name',
        },
        {
            title: '角色',
            dataIndex: 'character',
            key: 'character',
        },
        {
            title: '简介',
            dataIndex: 'bio',
            key: 'bio',
        },
    ];

    const mockCastData = [
        {
            key: '1',
            name: '主演1',
            character: '角色1',
            bio: '这是一位知名演员...',
        },
        {
            key: '2',
            name: '主演2',
            character: '角色2',
            bio: '这是一位知名演员...',
        },
    ];

    // Mock data for comments
    const commentColumns = [
        {
            title: '评论者',
            dataIndex: 'author',
            key: 'author',
        },
        {
            title: '评分',
            dataIndex: 'rating',
            key: 'rating',
            render: (rating: number) => (
                <Space>
                    <StarOutlined style={{ color: '#faad14' }} />
                    <Text>{rating}分</Text>
                </Space>
            ),
        },
        {
            title: '评论内容',
            dataIndex: 'content',
            key: 'content',
        },
    ];

    const mockCommentData = [
        {
            key: '1',
            author: '用户1',
            rating: 9.0,
            content: '这是一部非常棒的电影，强烈推荐！',
        },
        {
            key: '2',
            author: '用户2',
            rating: 7.5,
            content: '画面精美，剧情引人入胜。',
        },
    ];

    return (
        <div style={{ padding: 24, background: '#f0f2f5', minHeight: '100vh' }}>
            <div style={{ background: '#fff', padding: 24, borderRadius: 8 }}>
                <div style={{ textAlign: 'center', marginBottom: 24 }}>
                    <Title level={2}>{movieDetails.title}</Title>
                    <div style={{ marginTop: 16 }}>
                        <Tag icon={<StarOutlined />} color="gold">
                            评分: {movieDetails.rating}
                        </Tag>
                        <Tag icon={<CalendarOutlined />} color="blue">
                            上映日期: {movieDetails.releaseDate}
                        </Tag>
                        <Tag icon={<EyeOutlined />} color="green">
                            观看人数: {movieDetails.viewCount}
                        </Tag>
                    </div>
                </div>

                <Row gutter={[24, 24]}>
                    <Col xs={24} lg={16}>
                        <Card title="基本信息" style={{ marginBottom: 24 }}>
                            <Descriptions column={2} bordered size="small">
                                <Descriptions.Item label="电影海报">
                                    <img 
                                        src={movieDetails.posterUrl || '/placeholder-image.jpg'} 
                                        alt={movieDetails.title} 
                                        style={{ width: 120, height: 160, objectFit: 'cover' }} 
                                    />
                                </Descriptions.Item>
                                <Descriptions.Item label="导演">
                                    {movieDetails.director}
                                </Descriptions.Item>
                                <Descriptions.Item label="编剧">
                                    {movieDetails.writer}
                                </Descriptions.Item>
                                <Descriptions.Item label="类型">
                                    <Space>
                                        {movieDetails.genres?.map((genre, index) => (
                                            <Tag key={index} color="blue">{genre}</Tag>
                                        ))}
                                    </Space>
                                </Descriptions.Item>
                                <Descriptions.Item label="制片国家/地区">
                                    <Space>
                                        {movieDetails.countries?.map((country, index) => (
                                            <Tag key={index} icon={<EnvironmentOutlined />} color="default">{country}</Tag>
                                        ))}
                                    </Space>
                                </Descriptions.Item>
                                <Descriptions.Item label="语言">
                                    {movieDetails.languages?.join(', ')}
                                </Descriptions.Item>
                                <Descriptions.Item label="上映日期">
                                    {movieDetails.releaseDate}
                                </Descriptions.Item>
                                <Descriptions.Item label="片长">
                                    {movieDetails.runtime} 分钟
                                </Descriptions.Item>
                                <Descriptions.Item label="又名">
                                    {movieDetails.aka?.join(', ') || 'N/A'}
                                </Descriptions.Item>
                            </Descriptions>

                            <Divider>剧情简介</Divider>
                            <Text>{movieDetails.summary}</Text>
                        </Card>

                        <Card title="演员表" style={{ marginBottom: 24 }}>
                            <Table 
                                columns={castColumns} 
                                dataSource={mockCastData} 
                                pagination={{ pageSize: 5 }}
                            />
                        </Card>

                        <Card title="热门评论">
                            <Table 
                                columns={commentColumns} 
                                dataSource={mockCommentData} 
                                pagination={{ pageSize: 5 }}
                            />
                        </Card>
                    </Col>

                    <Col xs={24} lg={8}>
                        <Card title="评分详情" style={{ marginBottom: 24 }}>
                            <div style={{ textAlign: 'center', padding: '20px 0' }}>
                                <div style={{ fontSize: 36, fontWeight: 'bold', color: '#faad14', marginBottom: 8 }}>
                                    {movieDetails.rating}
                                </div>
                                <div style={{ color: '#888' }}>
                                    <StarOutlined /> 基于 {movieDetails.reviewCount} 个评价
                                </div>
                            </div>

                            <div style={{ marginTop: 20 }}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                                    <Text>10分</Text>
                                    <Text>{movieDetails.ratingDistribution?.['10'] || 0}%</Text>
                                </div>
                                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                                    <Text>9分</Text>
                                    <Text>{movieDetails.ratingDistribution?.['9'] || 0}%</Text>
                                </div>
                                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                                    <Text>8分</Text>
                                    <Text>{movieDetails.ratingDistribution?.['8'] || 0}%</Text>
                                </div>
                                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                                    <Text>7分</Text>
                                    <Text>{movieDetails.ratingDistribution?.['7'] || 0}%</Text>
                                </div>
                                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                                    <Text>6分</Text>
                                    <Text>{movieDetails.ratingDistribution?.['6'] || 0}%</Text>
                                </div>
                            </div>
                        </Card>

                        <Card title="标签">
                            <Space wrap>
                                {movieDetails.tags?.map((tag, index) => (
                                    <Tag key={index} color="purple">{tag}</Tag>
                                ))}
                            </Space>
                        </Card>
                    </Col>
                </Row>
            </div>
        </div>
    );
};