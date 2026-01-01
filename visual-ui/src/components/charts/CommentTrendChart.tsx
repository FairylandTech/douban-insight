import React from 'react';
import ReactECharts from 'echarts-for-react';
import { Card } from 'antd';
import type { TimeSeriesData } from '@/models/movie.model';

interface CommentTrendChartProps {
  data: TimeSeriesData[];
  loading?: boolean;
}

/**
 * Comment Trend Chart Component
 * Displays comment trends over time using a line chart
 */
export const CommentTrendChart: React.FC<CommentTrendChartProps> = ({
  data,
  loading = false,
}) => {
  const option = {
    title: {
      text: '评论趋势',
      left: 'center',
      textStyle: {
        fontSize: 16,
        fontWeight: 'bold',
      },
    },
    tooltip: {
      trigger: 'axis',
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      formatter: (params: any) => {
        let result = `${params[0].name}<br/>`;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        params.forEach((param: any) => {
          result += `${param.marker}${param.seriesName}: ${param.value}<br/>`;
        });
        return result;
      },
    },
    legend: {
      data: ['评论数', '平均评分'],
      top: 30,
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      containLabel: true,
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: data.map((d) => d.date),
    },
    yAxis: [
      {
        type: 'value',
        name: '评论数',
        position: 'left',
      },
      {
        type: 'value',
        name: '平均评分',
        position: 'right',
        min: 0,
        max: 10,
      },
    ],
    series: [
      {
        name: '评论数',
        type: 'line',
        data: data.map((d) => d.count),
        smooth: true,
        itemStyle: {
          color: '#1890ff',
        },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(24, 144, 255, 0.3)' },
              { offset: 1, color: 'rgba(24, 144, 255, 0.1)' },
            ],
          },
        },
      },
      {
        name: '平均评分',
        type: 'line',
        yAxisIndex: 1,
        data: data.map((d) => d.avgRating),
        smooth: true,
        itemStyle: {
          color: '#52c41a',
        },
      },
    ],
  };

  return (
    <Card loading={loading} style={{ height: '100%' }}>
      <ReactECharts option={option} style={{ height: '350px' }} />
    </Card>
  );
};
