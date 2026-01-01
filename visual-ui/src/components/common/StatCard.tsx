import React from 'react';
import { Card, Statistic } from 'antd';
import type { StatisticProps } from 'antd';

interface StatCardProps extends StatisticProps {
  icon?: React.ReactNode;
  loading?: boolean;
}

/**
 * Statistic Card Component
 * Displays a key metric with optional icon
 */
export const StatCard: React.FC<StatCardProps> = ({ icon, loading = false, ...props }) => {
  return (
    <Card loading={loading} style={{ height: '100%' }}>
      <Statistic {...props} prefix={icon} />
    </Card>
  );
};
