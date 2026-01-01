import React from 'react';
import {Row, Col, Typography, Alert, Spin} from 'antd';
import {
    VideoCameraOutlined,
    CommentOutlined,
    StarOutlined,
    SmileOutlined,
} from '@ant-design/icons';
import {useDashboardData} from '@/hooks/useMovieData';
import {StatCard} from '@/components/common/StatCard';
import {RatingDistributionChart} from '@/components/charts/RatingDistributionChart';
import {CommentTrendChart} from '@/components/charts/CommentTrendChart';
import {SentimentChart} from '@/components/charts/SentimentChart';
import {TopMoviesList} from '@/components/charts/TopMoviesList';

const {Title} = Typography;

/**
 * Dashboard Page Component
 * Main visualization page displaying movie statistics and insights
 */
export const Dashboard: React.FC = () => {
    const {dashboardStats, loading, error} = useDashboardData();

    if (error) {
        return (
            <div style={{padding: 24}}>
                <Alert message="加载失败" description={error} type="error" showIcon/>
            </div>
        );
    }

    if (loading || !dashboardStats) {
        return (
            <div
                style={{
                    display: 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                    height: '100vh',
                }}
            >
                <Spin size="large" tip="加载中..."/>
            </div>
        );
    }

    return (
        <div style={{padding: 24, background: '#f0f2f5', minHeight: '100vh'}}>
            <Title level={2} style={{marginBottom: 24}}>
                豆瓣电影数据可视化
            </Title>

            {/* Statistics Cards */}
            <Row gutter={[16, 16]} style={{marginBottom: 24}}>
                <Col xs={24} sm={12} lg={6}>
                    <StatCard
                        title="电影总数"
                        value={dashboardStats.totalMovies}
                        prefix={<VideoCameraOutlined style={{color: '#1890ff'}}/>}
                        loading={loading}
                    />
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <StatCard
                        title="评论总数"
                        value={dashboardStats.totalComments}
                        prefix={<CommentOutlined style={{color: '#52c41a'}}/>}
                        loading={loading}
                    />
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <StatCard
                        title="平均评分"
                        value={dashboardStats.avgRating}
                        precision={2}
                        prefix={<StarOutlined style={{color: '#faad14'}}/>}
                        loading={loading}
                    />
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <StatCard
                        title="正面评论占比"
                        value={
                            (dashboardStats.sentimentStats.positive / dashboardStats.totalComments) * 100
                        }
                        precision={1}
                        suffix="%"
                        prefix={<SmileOutlined style={{color: '#52c41a'}}/>}
                        loading={loading}
                    />
                </Col>
            </Row>

            {/* Charts Row 1 */}
            <Row gutter={[16, 16]} style={{marginBottom: 24}}>
                <Col xs={24} lg={12}>
                    <RatingDistributionChart
                        data={dashboardStats.ratingDistribution}
                        loading={loading}
                    />
                </Col>
                <Col xs={24} lg={12}>
                    <SentimentChart data={dashboardStats.sentimentStats} loading={loading}/>
                </Col>
            </Row>

            {/* Charts Row 2 */}
            <Row gutter={[16, 16]}>
                <Col xs={24} lg={16}>
                    <CommentTrendChart data={dashboardStats.commentTrends} loading={loading}/>
                </Col>
                <Col xs={24} lg={8}>
                    <TopMoviesList movies={dashboardStats.topMovies} loading={loading}/>
                </Col>
            </Row>
        </div>
    );
};
