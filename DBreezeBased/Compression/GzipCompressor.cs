/* 
  Copyright (C) 2014 dbreeze.tiesky.com / Alex Solovyov / Ivars Sudmalis.
  It's a free software for those, who thinks that it should be free.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

using DBreeze.Utils;

namespace DBreezeBased.Compression
{
    /// <summary>
    /// 
    /// </summary>
    public static class GzipCompressor
    {
        /// <summary>
        /// In Memory Compression with Gzip
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static byte[] CompressGZip(this byte[] data)
        {
            byte[] res = null;
            MemoryStream ms = null;
            System.IO.Compression.GZipStream gz = null;

            try
            {
                using (ms = new MemoryStream())
                {
                    using (gz = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Compress, false))
                    {
                        gz.Write(data, 0, data.Length);
                        gz.Close();
                    }

                    res = ms.ToArray();

                }
            }
            catch
            {
                res = null;
            }
            finally
            {
                if (gz != null)
                {
                    gz.Close();
                    gz.Dispose();
                }
                if (ms != null)
                {
                    ms.Close();
                    ms.Dispose();
                }
            }

            return res;
        }

        /// <summary>
        /// In Memory GZip Decompressor 
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static byte[] DecompressGZip(this byte[] data)
        {
            int length = 10000; //10Kb
            byte[] Ob = new byte[length];
            byte[] result = null;

            MemoryStream ms = null;
            System.IO.Compression.GZipStream gz = null;

            try
            {
                using (ms = new MemoryStream(data))
                {
                    using (gz = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress))
                    {
                        int a = 0;
                        while ((a = gz.Read(Ob, 0, length)) > 0)
                        {
                            if (a == length)
                                //result = Concat(result, Ob);
                                result = result.Concat(Ob);
                            else
                                //result = Concat(result, Substring(Ob, 0, a));
                                result = result.Concat(Ob.Substring(0, a));
                        }
                    }
                }
            }
            catch
            {
                result = null;
            }
            finally
            {
                if (gz != null)
                {
                    gz.Close();
                    gz.Dispose();
                }
                if (ms != null)
                {
                    ms.Close();
                    ms.Dispose();
                }
            }

            return result;
        }
    }
}
