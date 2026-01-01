import { ConfigProvider, theme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { Dashboard } from '@/pages/Dashboard';
import './App.css';

function App() {
  return (
    <ConfigProvider
      locale={zhCN}
      theme={{
        algorithm: theme.defaultAlgorithm,
        token: {
          colorPrimary: '#1890ff',
          borderRadius: 8,
        },
      }}
    >
      <Dashboard />
    </ConfigProvider>
  );
}

export default App;
