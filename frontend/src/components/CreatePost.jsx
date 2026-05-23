import { useState } from 'react';
import { uploadMedia, createPost } from '../api/client';
import { ImagePlus, Send, X, Loader2, CheckCircle2 } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

const CreatePost = () => {
  const [message, setMessage] = useState('');
  const [files, setFiles] = useState([]);
  const [previews, setPreviews] = useState([]);
  
  const [isUploading, setIsUploading] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [successMsg, setSuccessMsg] = useState('');
  const [errorMsg, setErrorMsg] = useState('');

  const handleFileChange = (e) => {
    const selectedFiles = Array.from(e.target.files);
    if (selectedFiles.length === 0) return;

    setFiles(prev => [...prev, ...selectedFiles]);
    
    //Preview Img
    const newPreviews = selectedFiles.map(file => URL.createObjectURL(file));
    setPreviews(prev => [...prev, ...newPreviews]);
  };

  const removeFile = (index) => {
    setFiles(prev => prev.filter((_, i) => i !== index));
    setPreviews(prev => prev.filter((_, i) => i !== index));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!message && files.length === 0) {
      setErrorMsg("Please write a message or attach an image.");
      return;
    }

    try {
      setErrorMsg('');
      setSuccessMsg('');
      
      let imageUrls = [];

      // 1. Upload Images to Backend (Cloudinary)
      if (files.length > 0) {
        setIsUploading(true);
        for (const file of files) {
          const res = await uploadMedia(file);
          if (res.success && res.data.url) {
            imageUrls.push(res.data.url);
          }
        }
        setIsUploading(false);
      }

      // 2. Create Post
      setIsSubmitting(true);
      const postData = {
        message,
        imageUrls
      };

      const result = await createPost(postData);
      
      if (result.success) {
        setSuccessMsg("Post created successfully on Facebook!");
        setMessage('');
        setFiles([]);
        setPreviews([]);
      }
    } catch (err) {
      setErrorMsg(err.message || "An error occurred while creating the post");
    } finally {
      setIsUploading(false);
      setIsSubmitting(false);
    }
  };

  return (
    <div className="max-w-2xl mx-auto">
      <header className="mb-8">
        <h2 className="text-3xl font-bold text-white mb-1">Create New Post</h2>
        <p className="text-textMuted">Publish content directly to your Facebook Page.</p>
      </header>

      <form onSubmit={handleSubmit} className="glass rounded-2xl p-6 border border-white/5 shadow-xl">
        <AnimatePresence>
          {successMsg && (
            <motion.div 
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              className="mb-6 p-4 bg-green-500/10 border border-green-500/20 rounded-xl flex items-center gap-3 text-green-400"
            >
              <CheckCircle2 className="w-5 h-5" />
              <p>{successMsg}</p>
            </motion.div>
          )}
          
          {errorMsg && (
            <motion.div 
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
              className="mb-6 p-4 bg-red-500/10 border border-red-500/20 rounded-xl flex items-center gap-3 text-red-400"
            >
              <p>{errorMsg}</p>
            </motion.div>
          )}
        </AnimatePresence>

        <div className="mb-6">
          <textarea
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            placeholder="What's on your mind?"
            className="w-full bg-black/20 border border-white/10 rounded-xl p-4 text-white placeholder-white/30 focus:outline-none focus:ring-2 focus:ring-primary/50 resize-none min-h-[150px] transition-all"
          />
        </div>

        {previews.length > 0 && (
          <div className="mb-6 flex gap-4 overflow-x-auto pb-2">
            {previews.map((preview, idx) => (
              <div key={idx} className="relative flex-shrink-0 group">
                <img src={preview} alt="Preview" className="w-32 h-32 object-cover rounded-xl border border-white/10" />
                <button
                  type="button"
                  onClick={() => removeFile(idx)}
                  className="absolute -top-2 -right-2 bg-red-500 text-white rounded-full p-1 opacity-0 group-hover:opacity-100 transition-opacity shadow-lg"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>
            ))}
          </div>
        )}

        <div className="flex items-center justify-between border-t border-white/10 pt-6">
          <div className="relative">
            <input 
              type="file" 
              multiple 
              accept="image/*"
              onChange={handleFileChange}
              className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
            />
            <button type="button" className="flex items-center gap-2 text-primary hover:text-primaryHover transition-colors font-medium px-4 py-2 bg-primary/10 rounded-lg">
              <ImagePlus className="w-5 h-5" />
              Add Photos
            </button>
          </div>

          <button
            type="submit"
            disabled={isUploading || isSubmitting}
            className="flex items-center gap-2 bg-primary hover:bg-primaryHover text-white px-6 py-2.5 rounded-xl font-semibold shadow-lg shadow-primary/30 transition-all disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isUploading ? (
              <><Loader2 className="w-5 h-5 animate-spin" /> Uploading...</>
            ) : isSubmitting ? (
              <><Loader2 className="w-5 h-5 animate-spin" /> Publishing...</>
            ) : (
              <><Send className="w-5 h-5" /> Publish Post</>
            )}
          </button>
        </div>
      </form>
    </div>
  );
};

export default CreatePost;
