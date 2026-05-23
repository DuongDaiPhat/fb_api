import { useState, useEffect } from 'react';
import { fetchPosts } from '../api/client';
import PostCard from './PostCard';
import { Loader2, AlertCircle } from 'lucide-react';
import { motion } from 'framer-motion';

const Dashboard = () => {
  const [posts, setPosts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadPosts();
  }, []);

  const loadPosts = async () => {
    try {
      setLoading(true);
      setError(null);
      const res = await fetchPosts();
      // Assume Graph API returns an object with a 'data' array containing posts
      if (res.success && res.data && res.data.data) {
        setPosts(res.data.data);
      } else {
        setPosts([]);
      }
    } catch (err) {
      setError(err.message || 'Failed to load posts');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-4xl mx-auto">
      <header className="mb-8 flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-white mb-1">Page Dashboard</h2>
          <p className="text-textMuted">Overview of your recent Facebook page posts and interactions.</p>
        </div>
        <button 
          onClick={loadPosts}
          className="px-4 py-2 glass rounded-lg hover:bg-white/10 transition-colors border border-white/10 text-sm font-medium"
        >
          Refresh
        </button>
      </header>

      {loading && (
        <div className="flex flex-col items-center justify-center py-20 text-primary">
          <Loader2 className="w-10 h-10 animate-spin mb-4" />
          <p className="text-textMuted">Syncing with Facebook...</p>
        </div>
      )}

      {error && !loading && (
        <motion.div 
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          className="p-4 bg-red-500/10 border border-red-500/20 rounded-xl flex items-center gap-3 text-red-400 mb-6"
        >
          <AlertCircle className="w-5 h-5 flex-shrink-0" />
          <p>{error}</p>
        </motion.div>
      )}

      {!loading && !error && posts.length === 0 && (
        <div className="text-center py-20 glass rounded-2xl border border-white/5">
          <p className="text-textMuted">No posts found on this page.</p>
        </div>
      )}

      <div className="space-y-6">
        {posts.map((post, index) => (
          <motion.div
            key={post.id}
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: index * 0.1 }}
          >
            <PostCard post={post} />
          </motion.div>
        ))}
      </div>
    </div>
  );
};

export default Dashboard;
