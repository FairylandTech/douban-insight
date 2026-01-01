import React from 'react';
import ReactECharts from 'echarts-for-react';
import { Card } from 'antd';
import type { RatingDistribution } from '@/models/movie.model';

interface RatingDistributionChartProps {
  data: RatingDistribution[];
  loading?: boolean;
}

/**
 * Rating Distribution Chart Component
 * Displays the distribution of ratings using a bar chart
 */
export const RatingDistributionChart: React.FC<RatingDistributionChartProps> = ({
  data,
  loading = false,
}) => {
  const option = {
    title: {
      text: '评分分布',
      left: 'center',
      textStyle: {
        fontSize: 16,
        fontWeight: 'bold',
      },
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'shadow',
      },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      formatter: (params: any) => {
        const item = params[0];
        return `${item.name}星<br/>数量: ${item.value}<br/>占比: ${data[item.dataIndex].percentage}%`;
      },
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      containLabel: true,
    },
    xAxis: {
      type: 'category',
      data: data.map((d) => `${d.rating}星`),
      axisLabel: {
        rotate: 0,
      },
    },
    yAxis: {
      type: 'value',
      name: '评论数',
    },
    series: [
      {
        name: '评论数',
        type: 'bar',
        data: data.map((d) => d.count),
        itemStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: '#1890ff' },
              { offset: 1, color: '#096dd9' },
            ],
          },
        },
        label: {
          show: true,
          position: 'top',
          formatter: '{c}',
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
