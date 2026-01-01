import React from 'react';
import ReactECharts from 'echarts-for-react';
import { Card } from 'antd';

interface SentimentChartProps {
  data: {
    positive: number;
    negative: number;
    neutral: number;
  };
  loading?: boolean;
}

/**
 * Sentiment Analysis Chart Component
 * Displays sentiment distribution using a pie chart
 */
export const SentimentChart: React.FC<SentimentChartProps> = ({ data, loading = false }) => {
  const option = {
    title: {
      text: '情感分析',
      left: 'center',
      textStyle: {
        fontSize: 16,
        fontWeight: 'bold',
      },
    },
    tooltip: {
      trigger: 'item',
      formatter: '{a} <br/>{b}: {c} ({d}%)',
    },
    legend: {
      orient: 'vertical',
      left: 'left',
      top: 50,
      data: ['正面', '负面', '中性'],
    },
    series: [
      {
        name: '情感分布',
        type: 'pie',
        radius: ['40%', '70%'],
        avoidLabelOverlap: false,
        itemStyle: {
          borderRadius: 10,
          borderColor: '#fff',
          borderWidth: 2,
        },
        label: {
          show: true,
          formatter: '{b}: {d}%',
        },
        emphasis: {
          label: {
            show: true,
            fontSize: 14,
            fontWeight: 'bold',
          },
        },
        data: [
          {
            value: data.positive,
            name: '正面',
            itemStyle: { color: '#52c41a' },
          },
          {
            value: data.negative,
            name: '负面',
            itemStyle: { color: '#ff4d4f' },
          },
          {
            value: data.neutral,
            name: '中性',
            itemStyle: { color: '#faad14' },
          },
        ],
      },
    ],
  };

  return (
    <Card loading={loading} style={{ height: '100%' }}>
      <ReactECharts option={option} style={{ height: '350px' }} />
    </Card>
  );
};
