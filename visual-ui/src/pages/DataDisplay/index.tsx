import React from 'react';
import { Row, Col, Typography, Card, Statistic, Space, Table, Tag, Avatar, Timeline } from 'antd';
import { 
    VideoCameraOutlined, 
    CommentOutlined, 
    StarOutlined, 
    LikeOutlined,
    UsergroupAddOutlined,
    RiseOutlined,
    CalendarOutlined,
    ClockCircleOutlined
} from '@ant-design/icons';
import { useDashboardData } from '@/hooks/useMovieData';
import { RatingDistributionChart } from '@/components/charts/RatingDistributionChart';
import { CommentTrendChart } from '@/components/charts/CommentTrendChart';
import { SentimentChart } from '@/components/charts/SentimentChart';
import { TopMoviesList } from '@/components/charts/TopMoviesList';
import type { Movie } from '@/models/movie.model';

const { Title, Text } = Typography;

/**
 * Data Display Page Component
 * Large screen visualization page with comprehensive movie statistics
 */
export const DataDisplay: React.FC = () => {
    const { dashboardStats, loading } = useDashboardData();

    // Mock data for recent activity timeline
    const timelineData = [
        { 
            key: '1', 
            time: '2026-01-01 10:30', 
            content: '《电影A》成为今日热门', 
            type: 'info' 
        },
        { 
            key: '2', 
            time: '2026-01-01 09:15', 
            content: '新增评论数量超过1万条', 
            type: 'success' 
        },
        { 
            key: '3', 
            time: '2026-01-01 08:45', 
            content: '用户活跃度达到峰值', 
            type: 'warning' 
        },
        { 
            key: '4', 
            time: '2026-01-01 07:20', 
            content: '新电影《电影B》上线', 
            type: 'info' 
        },
    ];

    // Mock data for top rated movies table
    const topMoviesColumns = [
        {
            title: '排名',
            dataIndex: 'rank',
            key: 'rank',
            render: (text: number) => (
                <Tag color={text === 1 ? 'gold' : text === 2 ? 'silver' : 'bronze'}>
                    {text}
                </Tag>
            ),
        },
        {
            title: '电影名称',
            dataIndex: 'title',
            key: 'title',
        },
        {
            title: '评分',
            dataIndex: 'rating',
            key: 'rating',
            render: (rating: number) => (
                <Space>
                    <StarOutlined style={{ color: '#faad14' }} />
                    <Text strong>{rating}</Text>
                </Space>
            ),
        },
        {
            title: '类型',
            dataIndex: 'genres',
            key: 'genres',
            render: (genres: string[]) => (
                <Space wrap>
                    {genres?.slice(0, 2).map((genre, index) => (
                        <Tag key={index} color="blue">
                            {genre}
                        </Tag>
                    ))}
                </Space>
            ),
        },
    ];

    // Mock data for top movies
    const topMoviesData = [
        { 
            key: '1', 
            rank: 1, 
            title: '肖申克的救赎', 
            rating: 9.7, 
            genres: ['剧情'] 
        },
        { 
            key: '2', 
            rank: 2, 
            title: '霸王别姬', 
            rating: 9.6, 
            genres: ['剧情', '爱情'] 
        },
        { 
            key: '3', 
            rank: 3, 
            title: '阿甘正传', 
            rating: 9.5, 
            genres: ['剧情', '爱情'] 
        },
        { 
            key: '4', 
            rank: 4, 
            title: '泰坦尼克号', 
            rating: 9.4, 
            genres: ['剧情', '爱情', '灾难'] 
        },
        { 
            key: '5', 
            rank: 5, 
            title: '这个杀手不太冷', 
            rating: 9.4, 
            genres: ['剧情', '动作', '犯罪'] 
        },
    ];

    // Mock data for genre distribution
    const genreData = [
        { name: '剧情', value: 25 },
        { name: '喜剧', value: 20 },
        { name: '动作', value: 15 },
        { name: '爱情', value: 12 },
        { name: '科幻', value: 10 },
        { name: '动画', value: 8 },
        { name: '悬疑', value: 7 },
        { name: '其他', value: 3 },
    ];

    return (
        <div 
          className="data-display-container"
          style={{ 
            padding: 16, 
            background: 'linear-gradient(135deg, #001529, #1e3c72)', 
            minHeight: 'calc(100vh - 130px)', // 减去头部高度
            color: '#fff',
            fontFamily: 'Arial, sans-serif'
        }}>
            <div style={{ 
                textAlign: 'center', 
                marginBottom: 32,
                padding: '20px 0'
            }}>
                <Title style={{ 
                    color: '#fff', 
                    fontSize: 36,
                    textShadow: '2px 2px 4px rgba(0,0,0,0.5)',
                    marginBottom: 8
                }}>
                    豆瓣电影数据分析大屏
                </Title>
                <Text style={{ fontSize: 16, color: 'rgba(255,255,255,0.8)' }}>
                    实时数据监控与可视化分析系统
                </Text>
            </div>

            {/* Main Stats Cards */}
            <Row gutter={[24, 24]} style={{marginBottom: 24}}>
                <Col xs={12} sm={6}>
                    <Card 
                        style={{ 
                            background: 'rgba(255,255,255,0.1)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.2)',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        bodyStyle={{ padding: '20px' }}
                    >
                        <div style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: 24, marginBottom: 12 }}>
                                <VideoCameraOutlined style={{ color: '#1890ff' }} />
                            </div>
                            <div style={{ fontSize: 16, color: 'rgba(255,255,255,0.8)', marginBottom: 8 }}>电影总数</div>
                            <div style={{ fontSize: 28, fontWeight: 'bold', color: '#fff' }}>
                                {dashboardStats?.totalMovies ? dashboardStats.totalMovies.toLocaleString() : '0'}
                            </div>
                        </div>
                    </Card>
                </Col>
                <Col xs={12} sm={6}>
                    <Card 
                        style={{ 
                            background: 'rgba(255,255,255,0.1)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.2)',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        bodyStyle={{ padding: '20px' }}
                    >
                        <div style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: 24, marginBottom: 12 }}>
                                <CommentOutlined style={{ color: '#52c41a' }} />
                            </div>
                            <div style={{ fontSize: 16, color: 'rgba(255,255,255,0.8)', marginBottom: 8 }}>评论总数</div>
                            <div style={{ fontSize: 28, fontWeight: 'bold', color: '#fff' }}>
                                {dashboardStats?.totalComments ? dashboardStats.totalComments.toLocaleString() : '0'}
                            </div>
                        </div>
                    </Card>
                </Col>
                <Col xs={12} sm={6}>
                    <Card 
                        style={{ 
                            background: 'rgba(255,255,255,0.1)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.2)',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        bodyStyle={{ padding: '20px' }}
                    >
                        <div style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: 24, marginBottom: 12 }}>
                                <StarOutlined style={{ color: '#faad14' }} />
                            </div>
                            <div style={{ fontSize: 16, color: 'rgba(255,255,255,0.8)', marginBottom: 8 }}>平均评分</div>
                            <div style={{ fontSize: 28, fontWeight: 'bold', color: '#fff' }}>
                                {dashboardStats?.avgRating ? dashboardStats.avgRating.toFixed(1) : '0.0'}
                            </div>
                        </div>
                    </Card>
                </Col>
                <Col xs={12} sm={6}>
                    <Card 
                        style={{ 
                            background: 'rgba(255,255,255,0.1)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.2)',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        bodyStyle={{ padding: '20px' }}
                    >
                        <div style={{ textAlign: 'center' }}>
                            <div style={{ fontSize: 24, marginBottom: 12 }}>
                                <RiseOutlined style={{ color: '#722ed1' }} />
                            </div>
                            <div style={{ fontSize: 16, color: 'rgba(255,255,255,0.8)', marginBottom: 8 }}>正面评论占比</div>
                            <div style={{ fontSize: 28, fontWeight: 'bold', color: '#fff' }}>
                                {dashboardStats 
                                    ? ((dashboardStats.sentimentStats.positive / dashboardStats.totalComments) * 100).toFixed(1) 
                                    : '0.0'}%
                            </div>
                        </div>
                    </Card>
                </Col>
            </Row>

            {/* Charts and Data Rows */}
            <Row gutter={[24, 24]} style={{ marginBottom: 24 }}>
                <Col xs={24} lg={14}>
                    <Card 
                        title="评分分布统计" 
                        style={{ 
                            background: 'rgba(255,255,255,0.05)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.1)',
                            height: '100%',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        headStyle={{ 
                            color: '#fff',
                            fontSize: 18,
                            borderBottom: '1px solid rgba(255,255,255,0.1)'
                        }}
                        bodyStyle={{ padding: 16 }}
                    >
                        <div style={{ height: 300 }}>
                            <RatingDistributionChart 
                                data={dashboardStats?.ratingDistribution || []} 
                                loading={loading} 
                            />
                        </div>
                    </Card>
                </Col>
                <Col xs={24} lg={10}>
                    <Card 
                        title="类型分布" 
                        style={{ 
                            background: 'rgba(255,255,255,0.05)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.1)',
                            height: '100%',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        headStyle={{ 
                            color: '#fff',
                            fontSize: 18,
                            borderBottom: '1px solid rgba(255,255,255,0.1)'
                        }}
                        bodyStyle={{ padding: 16 }}
                    >
                        <div style={{ 
                            display: 'flex', 
                            flexWrap: 'wrap', 
                            justifyContent: 'space-around',
                            height: '300px',
                            alignItems: 'center'
                        }}>
                            {genreData.map((item, index) => (
                                <div key={index} style={{ 
                                    textAlign: 'center', 
                                    margin: '10px',
                                    width: '80px'
                                }}>
                                    <div style={{ 
                                        width: 60, 
                                        height: 60, 
                                        borderRadius: '50%', 
                                        background: `hsl(${index * 60}, 70%, 60%)`,
                                        display: 'flex',
                                        alignItems: 'center',
                                        justifyContent: 'center',
                                        margin: '0 auto 8px'
                                    }}>
                                        <Text style={{ color: '#fff', fontWeight: 'bold' }}>{item.value}%</Text>
                                    </div>
                                    <Text style={{ color: '#fff' }}>{item.name}</Text>
                                </div>
                            ))}
                        </div>
                    </Card>
                </Col>
            </Row>

            <Row gutter={[24, 24]} style={{ marginBottom: 24 }}>
                <Col xs={24} lg={12}>
                    <Card 
                        title="评论趋势分析" 
                        style={{ 
                            background: 'rgba(255,255,255,0.05)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.1)',
                            height: '100%',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        headStyle={{ 
                            color: '#fff',
                            fontSize: 18,
                            borderBottom: '1px solid rgba(255,255,255,0.1)'
                        }}
                        bodyStyle={{ padding: 16 }}
                    >
                        <div style={{ height: 300 }}>
                            <CommentTrendChart 
                                data={dashboardStats?.commentTrends || []} 
                                loading={loading} 
                            />
                        </div>
                    </Card>
                </Col>
                <Col xs={24} lg={12}>
                    <Card 
                        title="情感分析" 
                        style={{ 
                            background: 'rgba(255,255,255,0.05)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.1)',
                            height: '100%',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        headStyle={{ 
                            color: '#fff',
                            fontSize: 18,
                            borderBottom: '1px solid rgba(255,255,255,0.1)'
                        }}
                        bodyStyle={{ padding: 16 }}
                    >
                        <div style={{ height: 300 }}>
                            <SentimentChart 
                                data={dashboardStats?.sentimentStats} 
                                loading={loading} 
                            />
                        </div>
                    </Card>
                </Col>
            </Row>

            {/* Additional Data Rows */}
            <Row gutter={[24, 24]}>
                <Col xs={24} lg={12}>
                    <Card 
                        title="热门电影TOP榜" 
                        style={{ 
                            background: 'rgba(255,255,255,0.05)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.1)',
                            height: '100%',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        headStyle={{ 
                            color: '#fff',
                            fontSize: 18,
                            borderBottom: '1px solid rgba(255,255,255,0.1)'
                        }}
                        bodyStyle={{ padding: 16 }}
                    >
                        <div style={{ height: 300 }}>
                            <Table 
                                columns={topMoviesColumns} 
                                dataSource={topMoviesData} 
                                pagination={false}
                                showHeader={false}
                                size="small"
                                rowStyle={{ background: 'transparent', color: '#fff' }}
                                style={{ background: 'transparent' }}
                            />
                        </div>
                    </Card>
                </Col>
                <Col xs={24} lg={12}>
                    <Card 
                        title="实时动态" 
                        style={{ 
                            background: 'rgba(255,255,255,0.05)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.1)',
                            height: '100%',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        headStyle={{ 
                            color: '#fff',
                            fontSize: 18,
                            borderBottom: '1px solid rgba(255,255,255,0.1)'
                        }}
                        bodyStyle={{ padding: 16 }}
                    >
                        <Timeline 
                            mode="left"
                            items={timelineData.map(item => ({
                                children: (
                                    <div>
                                        <Text type="secondary">{item.time}</Text>
                                        <div style={{ marginTop: 8 }}>{item.content}</div>
                                    </div>
                                ),
                                color: item.type === 'success' ? 'green' : 
                                       item.type === 'warning' ? 'orange' : 
                                       item.type === 'error' ? 'red' : 'blue'
                            }))}
                        />
                    </Card>
                </Col>
            </Row>

            {/* Footer Stats */}
            <Row gutter={[24, 24]} style={{ marginTop: 24 }}>
                <Col xs={24} sm={8}>
                    <Card 
                        style={{ 
                            background: 'rgba(255,255,255,0.1)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.2)',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        bodyStyle={{ padding: '20px' }}
                    >
                        <div style={{ textAlign: 'center' }}>
                            <UsergroupAddOutlined style={{ fontSize: 32, color: '#1890ff', marginBottom: 12 }} />
                            <div style={{ fontSize: 16, color: 'rgba(255,255,255,0.8)', marginBottom: 8 }}>活跃用户数</div>
                            <div style={{ fontSize: 28, fontWeight: 'bold', color: '#fff' }}>
                                {dashboardStats?.totalComments ? Math.floor(dashboardStats.totalComments * 0.15).toLocaleString() : '0'}
                            </div>
                        </div>
                    </Card>
                </Col>
                <Col xs={24} sm={8}>
                    <Card 
                        style={{ 
                            background: 'rgba(255,255,255,0.1)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.2)',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        bodyStyle={{ padding: '20px' }}
                    >
                        <div style={{ textAlign: 'center' }}>
                            <LikeOutlined style={{ fontSize: 32, color: '#52c41a', marginBottom: 12 }} />
                            <div style={{ fontSize: 16, color: 'rgba(255,255,255,0.8)', marginBottom: 8 }}>好评数</div>
                            <div style={{ fontSize: 28, fontWeight: 'bold', color: '#fff' }}>
                                {dashboardStats?.sentimentStats?.positive ? dashboardStats.sentimentStats.positive.toLocaleString() : '0'}
                            </div>
                        </div>
                    </Card>
                </Col>
                <Col xs={24} sm={8}>
                    <Card 
                        style={{ 
                            background: 'rgba(255,255,255,0.1)', 
                            borderRadius: 12,
                            border: '1px solid rgba(255,255,255,0.2)',
                            boxShadow: '0 4px 12px rgba(0,0,0,0.1)'
                        }}
                        bodyStyle={{ padding: '20px' }}
                    >
                        <div style={{ textAlign: 'center' }}>
                            <VideoCameraOutlined style={{ fontSize: 32, color: '#faad14', marginBottom: 12 }} />
                            <div style={{ fontSize: 16, color: 'rgba(255,255,255,0.8)', marginBottom: 8 }}>本周新增电影</div>
                            <div style={{ fontSize: 28, fontWeight: 'bold', color: '#fff' }}>
                                {dashboardStats?.totalMovies ? Math.floor(dashboardStats.totalMovies * 0.02).toLocaleString() : '0'}
                            </div>
                        </div>
                    </Card>
                </Col>
            </Row>
        </div>
    );
};