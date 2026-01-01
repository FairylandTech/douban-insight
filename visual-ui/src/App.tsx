import { ConfigProvider, theme, Layout, Menu } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { BrowserRouter, Routes, Route, Navigate, useNavigate } from 'react-router-dom';
import { Dashboard } from '@/pages/Dashboard';
import { MovieDetail } from '@/pages/MovieDetail';
import { DataDisplay } from '@/pages/DataDisplay';
import { MovieList } from '@/pages/MovieList';
import './App.css';

const { Header, Content } = Layout;

const AppContent = () => {
  const navigate = useNavigate();

  const menuItems = [
    { key: '/', label: '数据大屏' },
    { key: '/dashboard', label: '数据概览' },
    { key: '/movielist', label: '电影列表' },
  ];

  const handleMenuClick = (e: { key: string }) => {
    navigate(e.key);
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Header style={{ padding: '0 20px', background: '#fff', borderBottom: '1px solid #e8e8e8' }}>
        <div style={{ float: 'left', marginRight: 20 }}>
          <h1 style={{ margin: 0, fontSize: 18, fontWeight: 'bold' }}>豆瓣电影分析系统</h1>
        </div>
        <Menu
          mode="horizontal"
          defaultSelectedKeys={['/']}
          style={{ flex: 1, minWidth: 0, background: 'transparent', border: 'none' }}
          items={menuItems}
          onClick={handleMenuClick}
        />
      </Header>
      <Content style={{ padding: '24px', background: '#f0f2f5' }}>
        <Routes>
          <Route path="/" element={<DataDisplay />} />
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/datadisplay" element={<DataDisplay />} />
          <Route path="/movielist" element={<MovieList />} />
          <Route path="/moviedetail/:id?" element={<MovieDetail />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Content>
    </Layout>
  );
};

function App() {
  return (
    <BrowserRouter>
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
        <AppContent />
      </ConfigProvider>
    </BrowserRouter>
  );
}

export default App;