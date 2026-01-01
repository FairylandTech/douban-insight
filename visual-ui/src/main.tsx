import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import App from './App.tsx';

// Enable MSW in development mode
const enableMocking = async () => {
  const isMockEnabled = import.meta.env.VITE_ENABLE_MOCK === 'true';

  if (isMockEnabled && import.meta.env.DEV) {
    try {
      const { worker } = await import('./mocks/browser');
      await worker.start({
        onUnhandledRequest: 'bypass',
      });
      console.log('MSW started successfully');
    } catch (error) {
      console.error('MSW initialization failed:', error);
    }
  }
};

enableMocking().then(() => {
  console.log('Rendering app...');
  const rootElement = document.getElementById('root');
  if (!rootElement) {
    console.error('Root element not found!');
    return;
  }
  createRoot(rootElement).render(
    <StrictMode>
      <App />
    </StrictMode>
  );
  console.log('App rendered');
});
