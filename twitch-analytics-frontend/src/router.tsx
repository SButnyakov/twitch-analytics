import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import SearchPage from './pages/search';
import StreamerPage from './pages/streamer';
import GamePage from './pages/game';
import NotFoundPage from './pages/notfound';

const AppRouter: React.FC = () => {
    return (
      <Router>
        <Routes>
          <Route path='/' element={<SearchPage/>} />
          <Route path='/streamer/:id' element={<StreamerPage/>} />
          <Route path='/game/:id' element={<GamePage/>} />
          <Route path='/*' element={<NotFoundPage/>} />
        </Routes>
      </Router>
    );
}
  
export default AppRouter;