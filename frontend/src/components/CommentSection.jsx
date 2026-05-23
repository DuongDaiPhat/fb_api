import { useState } from 'react';
import { fetchComments } from '../api/client';
import { MessageSquare, Loader2, User } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';

const CommentSection = ({ postId }) => {
  const [comments, setComments] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [isOpen, setIsOpen] = useState(false);

  const toggleComments = async () => {
    if (!isOpen && comments.length === 0) {
      try {
        setLoading(true);
        const res = await fetchComments(postId);
        if (res.success && res.data && res.data.data) {
          setComments(res.data.data);
        }
      } catch (err) {
        setError("Failed to load comments");
      } finally {
        setLoading(false);
      }
    }
    setIsOpen(!isOpen);
  };

  return (
    <div className="mt-4 border-t border-white/10 pt-4">
      <button 
        onClick={toggleComments}
        className="flex items-center gap-2 text-textMuted hover:text-primary transition-colors text-sm font-medium"
      >
        <MessageSquare className="w-4 h-4" />
        {isOpen ? 'Hide Comments' : 'View Comments'}
      </button>

      {isOpen && (
        <div className="mt-4 space-y-4">
          {loading && (
            <div className="flex items-center gap-2 text-textMuted text-sm">
              <Loader2 className="w-4 h-4 animate-spin" /> Loading...
            </div>
          )}
          {error && <p className="text-red-400 text-sm">{error}</p>}
          {!loading && !error && comments.length === 0 && (
            <p className="text-textMuted text-sm italic">No comments yet.</p>
          )}
          
          <div className="space-y-3">
            {comments.map((comment) => (
              <div key={comment.id} className="flex gap-3 bg-black/20 p-3 rounded-lg border border-white/5">
                <div className="w-8 h-8 rounded-full bg-surface flex items-center justify-center flex-shrink-0">
                  <User className="w-4 h-4 text-textMuted" />
                </div>
                <div>
                  <div className="flex items-baseline gap-2 mb-1">
                    <span className="font-semibold text-sm text-white">{comment.from?.name || 'Unknown User'}</span>
                    {comment.created_time && (
                      <span className="text-xs text-textMuted">
                        {formatDistanceToNow(new Date(comment.created_time), { addSuffix: true })}
                      </span>
                    )}
                  </div>
                  <p className="text-sm text-textMain">{comment.message}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default CommentSection;
