import { Calendar, Globe } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';
import CommentSection from './CommentSection';

const PostCard = ({ post }) => {
  return (
    <div className="glass rounded-2xl p-6 border border-white/5 shadow-lg">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-full bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center shadow-inner">
            <span className="font-bold text-white">FB</span>
          </div>
          <div>
            <h3 className="font-semibold text-white">Page Post</h3>
            <div className="flex items-center gap-2 text-xs text-textMuted">
              <Calendar className="w-3 h-3" />
              <span>
                {post.created_time ? formatDistanceToNow(new Date(post.created_time), { addSuffix: true }) : 'Just now'}
              </span>
              <span>•</span>
              <Globe className="w-3 h-3" />
            </div>
          </div>
        </div>
      </div>

      <div className="mb-4">
        <p className="text-textMain whitespace-pre-wrap">{post.message}</p>
      </div>

      {post.full_picture && (
        <div className="mb-4 rounded-xl overflow-hidden border border-white/10">
          <img src={post.full_picture} alt="Post media" className="w-full h-auto object-cover max-h-96" />
        </div>
      )}

      {/* Render attachments if any */}
      {post.attachments?.data && post.attachments.data.length > 0 && !post.full_picture && (
        <div className="mb-4 grid grid-cols-2 gap-2">
           {post.attachments.data[0]?.subattachments?.data?.map((media, idx) => (
             <div key={idx} className="rounded-lg overflow-hidden border border-white/10">
                <img src={media.media.image.src} className="w-full h-40 object-cover" alt="attachment" />
             </div>
           ))}
        </div>
      )}

      <CommentSection postId={post.id} />
    </div>
  );
};

export default PostCard;
