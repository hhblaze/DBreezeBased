/* 
  Copyright (C) 2014 dbreeze.tiesky.com / Alex Solovyov / Ivars Sudmalis.
  It's a free software for those, who thinks that it should be free.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
//using System.Threading.Tasks;

using DBreeze;
using DBreeze.Utils;
using DBreezeBased.Serialization;
using DBreezeBased.Compression;


namespace DBreezeBased.DocumentsStorage
{
    /// <summary>
    /// Main class to start DocumentsStorage operations
    /// </summary>
    public class Storage
    {
        /// <summary>
        /// 
        /// </summary>
        const string MyName = "DBreezeBased.DocumentsStorage.Storage";
        /// <summary>
        /// DBreeze table name prefix. Default dcstr. Tables concerning DBreezeBased.DocumentsStorage will start from this prefix
        /// </summary>
        public string DocumentsStorageTablesPrefix = "dcstr";

        /// <summary>
        /// Must stay 2 if you want to search starting from 2 letters or bigger in other case.
        /// Minimal lenght of the search word among the document.
        /// Document searchables will be prepared due to this value
        /// </summary>
        public int SearchWordMinimalLength = 2;

        /// <summary>
        /// Engine processes newly added documents by chunks limited by quantity of chars.
        /// More chars - more RAM for one block processing. Default value is 10MLN chars.
        /// Probably for mobile telephones this value must be decreased to 100K.
        /// </summary>
        public int MaxCharsToBeProcessedPerRound = 10000000;

        /// <summary>
        /// 
        /// </summary>
        public event Action OnProcessingStarted;

        /// <summary>
        /// 
        /// </summary>
        public event Action OnProcessingStopped;

        /// <summary>
        /// DBreeze engine must be supplied
        /// </summary>
        DBreezeEngine DBreezeEngine = null;

        /// <summary>
        /// Search Index setting. Less value - bigger index on the disc, and faster search
        /// </summary>
        public int QuantityOfWordsInBlock = 1000;
        /// <summary>
        /// Search Index setting. 
        /// </summary>
        public int MinimalBlockReservInBytes = 100000;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="methodName"></param>
        /// <param name="content"></param>
        internal Exception ThrowException(string methodName, string content)
        {
            return new Exception(String.Format("{0}.{1}: {2}",MyName,methodName,content));
        }


        /// <summary>
        /// Constructor (automatically starts unfinished indexing job)
        /// </summary>
        /// <param name="DBreezeEngine">must be already initialized</param>
        public Storage(DBreezeEngine DBreezeEngine)
        {
            this.OnProcessingStarted += Storage_OnProcessingStarted;
            this.OnProcessingStopped += Storage_OnProcessingStopped;
            if (DBreezeEngine == null)
                throw ThrowException("Storage", "DBreezeEngine must be instantiated"); 
            //if(SearchWordMinimalLength < 1)
            //    throw ThrowException("Storage", "SearchWordMinimalLength must be > 0");
            //if (DocumentsStorageTablesPrefix.Length < 1)
            //    throw ThrowException("Storage", "DocumentsStorageTablesPrefix.Length must be > 0");

            this.DBreezeEngine = DBreezeEngine;

            //Preparing Protobuf
            ProtoBuf.Serializer.PrepareSerializer<Document>();
            ProtoBuf.Serializer.PrepareSerializer<SearchRequest>();
            ProtoBuf.Serializer.PrepareSerializer<SearchResponse>();
            Document o1 = new Document();
            o1.SerializeProtobuf();
            SearchRequest o2 = new SearchRequest();
            o2.SerializeProtobuf();
            SearchResponse o3 = new SearchResponse();
            o3.SerializeProtobuf();


            //Automatic indexing of unfinished documents
            StartDocumentsIndexing();
          
        }

        /// <summary>
        /// Empty subscriber
        /// </summary>
        void Storage_OnProcessingStopped()
        {
            
        }

        /// <summary>
        /// Empty subscriber
        /// </summary>
        void Storage_OnProcessingStarted()
        {
            
        }
                
        /// <summary>
        /// Tech calss
        /// </summary>
        class I1
        {
            public DBreeze.DataTypes.NestedTable dt { get; set; }   //document table
            public DBreeze.DataTypes.NestedTable vt { get; set; }   //version table
            public DBreeze.DataTypes.NestedTable et { get; set; }   //externalID table
            public int MaxDocId = 0;
            public string DocTableName = "";

        }

        /// <summary>
        /// Deletes "s" and "p" tables from every document space and recreates search indexes for all non-deleted documents in all document spaces.
        /// </summary>
        public void ReindexDocuments()
        {
            try
            {
                long MaxDocSpaceId = 0;
                using (var tran = DBreezeEngine.GetTransaction())
                {
                    //Getting all document spaces
                    MaxDocSpaceId = tran.Select<int, long>(DocumentsStorageTablesPrefix + "m", 2).Value;

                    if (MaxDocSpaceId == 0)
                        return;

                    //string tblN = DBreezeEngine.Scheme.GetTablePathFromTableName(DocumentsStorageTablesPrefix + "s1");
                    //string tblN1 = DBreezeEngine.Scheme.GetTablePathFromTableName(DocumentsStorageTablesPrefix + "p");
                    

                    //removing all search indexes
                    for (int docSpace = 1; docSpace <= MaxDocSpaceId; docSpace++)
                    {
                        tran.RemoveAllKeys(DocumentsStorageTablesPrefix +  "s" + docSpace.ToString(), true);
                    }

                    tran.RemoveAllKeys(DocumentsStorageTablesPrefix + "p", true);
                
                }

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    tran.SynchronizeTables(DocumentsStorageTablesPrefix + "p");

                    for (long docSpaceId = 1; docSpaceId <= MaxDocSpaceId; docSpaceId++)
                    {
                        var dt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "d" + docSpaceId, 1, 0);

                        foreach (var row in dt.SelectForward<int, byte[]>())
                        {
                            tran.Insert<byte[], byte>(DocumentsStorageTablesPrefix + "p", docSpaceId.To_8_bytes_array_BigEndian().Concat(row.Key.To_4_bytes_array_BigEndian()), 0);
                        }

                        dt.Dispose();
                        dt = null;
                    }

                    tran.Commit();
                }

                this.StartDocumentsIndexing();
            }
            catch (Exception ex)
            {
                
            }
        }

        /// <summary>
        /// To be used inside of ready transaction.
        /// All table syncs must be already done
        /// </summary>
        public class InTran_DocumentAppender
        {
            DBreeze.Transactions.Transaction tran = null;
            //long documentSpaceId = 0;
            string DocumentsStorageTablesPrefix = "";
            Dictionary<long, I1> h = new Dictionary<long, I1>();
            byte[] serDoc = null;
            byte[] btDoc = null;
            I1 dhl = null;

            /// <summary>
            /// 
            /// </summary>
            /// <param name="_tran"></param>
            /// <param name="_DocumentsStorageTablesPrefix"></param>
            public InTran_DocumentAppender(DBreeze.Transactions.Transaction _tran, string _DocumentsStorageTablesPrefix)
            {
                if(_tran == null)
                    throw new Exception("DocumentAppender transaction is null");

                tran = _tran;
                //documentSpaceId = _documentSpaceId;
                DocumentsStorageTablesPrefix = _DocumentsStorageTablesPrefix;
            }


            /// <summary>
            /// Supplied document must already have DocumentSpaceId filled.
            /// Must be Synced tables: DocumentsStorageTablesPrefix + "p", and all DocumentsStorageTablesPrefix + "d" + doc.DocumentSpaceId
            /// </summary>
            /// <param name="doc"></param>
            public void RemoveDocument(Document doc)
            {
                if (doc == null || (doc.InternalId < 1 && String.IsNullOrEmpty(doc.ExternalId)))
                    throw new Exception("Check supplied Internal or External ID's and Document Space Id");

                if (!h.TryGetValue(doc.DocumentSpaceId, out dhl))
                {
                    dhl = new I1()
                    {
                        DocTableName = DocumentsStorageTablesPrefix + "d" + doc.DocumentSpaceId.ToString()
                    };

                    //document table
                    dhl.dt = tran.InsertTable<int>(dhl.DocTableName, 1, 0);
                    //Version table Key is composite InitialDocId(int)+VersionNumber(int)+SequentialDocId(int)
                    dhl.vt = tran.InsertTable<int>(dhl.DocTableName, 3, 0);
                    dhl.et = tran.InsertTable<int>(dhl.DocTableName, 2, 0); //ExternalId to InternalId relation
                   // dhl.MaxDocId = tran.Select<int, int>(dhl.DocTableName, 4).Value;
                    h[doc.DocumentSpaceId] = dhl;
                }


                dhl.vt.ValuesLazyLoadingIsOn = false;

                if (doc.InternalId < 1)
                {
                    if (!String.IsNullOrEmpty(doc.ExternalId))  //Getting internalId via external
                    {
                        doc.InternalId = dhl.et.Select<string, int>(doc.ExternalId).Value;
                    }

                    if (doc.InternalId < 1)
                        return;
                }

                //Iterating through all versions of the document
                foreach (var vtRow in dhl.vt.SelectBackwardFromTo<byte[], byte>
                   (
                   doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MaxValue.To_4_bytes_array_BigEndian(), int.MaxValue.To_4_bytes_array_BigEndian()), true,
                   doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MinValue.To_4_bytes_array_BigEndian(), int.MinValue.To_4_bytes_array_BigEndian()), true
                   , true
                   ))
                {
                    if (vtRow.Value == 0)
                    {
                        //Including the last one into processing list with the value 1 (to be deleted)
                        tran.Insert<byte[], byte>(DocumentsStorageTablesPrefix + "p", doc.DocumentSpaceId.To_8_bytes_array_BigEndian().Concat(vtRow.Key.Substring(8, 4)), 1);
                    }
                    break;
                }

            }

            /// <summary>
            /// Supplied document must already have DocumentSpaceId filled.
            /// Must be Synced tables: DocumentsStorageTablesPrefix + "p", and all DocumentsStorageTablesPrefix + "d" + doc.DocumentSpaceId
            /// </summary>
            /// <param name="doc"></param>
            public void AppendDocument(Document doc)
            {
                
                if (!h.TryGetValue(doc.DocumentSpaceId, out dhl))
                {
                    dhl = new I1()
                    {
                        DocTableName = DocumentsStorageTablesPrefix + "d" + doc.DocumentSpaceId.ToString()
                    };

                    //document table
                    dhl.dt = tran.InsertTable<int>(dhl.DocTableName, 1, 0);
                    //Version table Key is composite InitialDocId(int)+VersionNumber(int)+SequentialDocId(int)
                    dhl.vt = tran.InsertTable<int>(dhl.DocTableName, 3, 0);
                    dhl.et = tran.InsertTable<int>(dhl.DocTableName, 2, 0); //ExternalId to InternalId relation
                    dhl.MaxDocId = tran.Select<int, int>(dhl.DocTableName, 4).Value;

                    h[doc.DocumentSpaceId] = dhl;
                }


                //Increasing maximal docIndex in the docSpace
                dhl.MaxDocId++;
                tran.Insert<int, int>(dhl.DocTableName, 4, dhl.MaxDocId);

                //Saving doc content separately and repack instead SelectDirect link
                if (doc.Content != null)
                {
                    //16 bytes link to Content
                    doc.Content = dhl.dt.InsertDataBlock(null, doc.Content);
                }

                //Extra compressing searchables routine.
                if (!String.IsNullOrEmpty(doc.Searchables))
                {
                    byte[] btSearchables = System.Text.Encoding.UTF8.GetBytes(doc.Searchables);
                    byte[] btSearchablesZipped = btSearchables.CompressGZip();
                    if (btSearchablesZipped.Length < btSearchables.Length)
                        btSearchables = new byte[] { 1 }.Concat(btSearchablesZipped);
                    else
                        btSearchables = new byte[] { 0 }.Concat(btSearchables);

                    doc.InternalStructure = dhl.dt.InsertDataBlock(null, btSearchables);

                    //Now document is lightweight, without real content and searchables
                    doc.Searchables = null;
                }


                if (!String.IsNullOrEmpty(doc.ExternalId))
                {
                    //If externalID is supplied, we use it to retrieve internal id
                    doc.InternalId = dhl.et.Select<string, int>(doc.ExternalId).Value;

                    if (doc.InternalId == 0)
                    {
                        //New doc
                        doc.DocumentSequentialId = dhl.MaxDocId;
                        doc.InternalId = doc.DocumentSequentialId;
                        dhl.et.Insert<string, int>(doc.ExternalId, doc.InternalId);
                        //Inserting into version table 
                        //Console.WriteLine("Adding_V_" + doc.InternalId + "_1_" + doc.DocumentSequentialId);
                        dhl.vt.Insert<byte[], byte[]>(doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(((int)1).To_4_bytes_array_BigEndian(), doc.DocumentSequentialId.To_4_bytes_array_BigEndian()), new byte[] { 0 });
                    }
                    else
                    {
                        //Updating document (we create new version)
                        doc.DocumentSequentialId = dhl.MaxDocId;
                        //Getting version number
                        foreach (var row in dhl.vt.SelectBackwardFromTo<byte[], byte>(
                            doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MaxValue.To_4_bytes_array_BigEndian(), int.MaxValue.To_4_bytes_array_BigEndian()), true,
                            doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MinValue.To_4_bytes_array_BigEndian(), int.MinValue.To_4_bytes_array_BigEndian()), true
                            ))
                        {
                            //Inserting into version table, new version
                            //Console.WriteLine("Adding_V_" + doc.InternalId + "_" + (row.Key.Substring(4, 4).To_Int32_BigEndian() + 1) + "_" + doc.DocumentSequentialId);
                            dhl.vt.Insert<byte[], byte[]>(doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany
                                (
                                    (row.Key.Substring(4, 4).To_Int32_BigEndian() + 1).To_4_bytes_array_BigEndian(),
                                    doc.DocumentSequentialId.To_4_bytes_array_BigEndian()
                                ), new byte[] { 0 });
                            break;
                        }

                    }
                }
                else
                {
                    if (doc.InternalId < 1)
                    {
                        //New doc
                        doc.DocumentSequentialId = dhl.MaxDocId;
                        doc.InternalId = doc.DocumentSequentialId;
                        //Inserting into version table
                        //Console.WriteLine("Adding_V_" + doc.InternalId + "_1_" + doc.DocumentSequentialId);
                        dhl.vt.Insert<byte[], byte[]>(doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(((int)1).To_4_bytes_array_BigEndian(), doc.DocumentSequentialId.To_4_bytes_array_BigEndian()), new byte[] { 0 });
                    }
                    else
                    {
                        //Updating document (we create new version)
                        doc.DocumentSequentialId = dhl.MaxDocId;
                        //Getting version number
                        foreach (var row in
                            dhl.vt.SelectBackwardFromTo<byte[], byte>(
                            doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MaxValue.To_4_bytes_array_BigEndian(), int.MaxValue.To_4_bytes_array_BigEndian()), true,
                            doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MinValue.To_4_bytes_array_BigEndian(), int.MinValue.To_4_bytes_array_BigEndian()), true
                            ))
                        {
                            //Inserting into version table, new version
                            //Console.WriteLine("Adding_V_" + doc.InternalId + "_" + (row.Key.Substring(4, 4).To_Int32_BigEndian() + 1) + "_" + doc.DocumentSequentialId);
                            dhl.vt.Insert<byte[], byte[]>(doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany
                                (
                                    (row.Key.Substring(4, 4).To_Int32_BigEndian() + 1).To_4_bytes_array_BigEndian(),
                                    doc.DocumentSequentialId.To_4_bytes_array_BigEndian()
                                ), new byte[] { 0 });
                            break;
                        }
                    }
                }

                serDoc = doc.SerializeProtobuf();
                btDoc = serDoc.CompressGZip();

                if (serDoc.Length >= btDoc.Length)
                    btDoc = new byte[] { 1 }.Concat(btDoc);
                else
                    btDoc = new byte[] { 0 }.Concat(serDoc);
                dhl.dt.Insert<int, byte[]>(doc.DocumentSequentialId, btDoc);

                //-----------------------------------------------------------------------------------------

                //Adding to processing table
                //Console.WriteLine("Adding_P_" + doc.DocumentSequentialId);
                tran.Insert<byte[], byte>(DocumentsStorageTablesPrefix + "p", doc.DocumentSpaceId.To_8_bytes_array_BigEndian().Concat(doc.DocumentSequentialId.To_4_bytes_array_BigEndian()), 0);
            }


        }//End of DocumentAppender class

        /// <summary>
        /// Adds documents to the storage.
        /// </summary>
        /// <param name="docs">Documents to  be added</param>
        /// <returns>Can return empty list in case of misunderstandings, or the same list back with cleaned Searchables, Content and set InternalId</returns>
        //public List<Document> AddDocuments(List<Document> docs)
        public IList<Document> AddDocuments(IList<Document> docs)
        {

            try
            {
                if (docs == null)
                    throw ThrowException("AddDocuments", "supplied document is null");

                if (docs.Count() == 0)
                    return new List<Document>();


                HashSet<string> syncroTables = new HashSet<string>();

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    tran.SynchronizeTables(DocumentsStorageTablesPrefix + "m");
                    var mt = tran.InsertTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
                    long maxDocSpaceId = tran.Select<int, long>(DocumentsStorageTablesPrefix + "m", 2).Value;

                    foreach (var doc in docs)
                    {
                        if (String.IsNullOrEmpty(doc.DocumentSpace))
                            throw ThrowException("AddDocuments", "supplied Document Space can't be empty");

                        if (String.IsNullOrEmpty(doc.Searchables))
                        {
                            //throw ThrowException("AddDocuments", "Document Searchables can't be empty");
                        }
                        else if (doc.Searchables.Length > 50000000)
                            throw ThrowException("AddDocuments", "Document Searchables is bigger then 50MLN symbols"); //Can be changed, if necessary

                        doc.DocumentSpaceId = mt.Select<string, long>(doc.DocumentSpace).Value;

                        if (doc.DocumentSpaceId == 0)
                        {
                            //Creating document space
                            maxDocSpaceId++;
                            doc.DocumentSpaceId = maxDocSpaceId;
                            tran.Insert<int, long>(DocumentsStorageTablesPrefix + "m", 2, maxDocSpaceId);
                            mt.Insert<string, long>(doc.DocumentSpace, doc.DocumentSpaceId);
                        }

                        if (!syncroTables.Contains(DocumentsStorageTablesPrefix + "d" + doc.DocumentSpaceId.ToString()))
                        {
                            syncroTables.Add(DocumentsStorageTablesPrefix + "d" + doc.DocumentSpaceId.ToString());
                        }
                    }

                    tran.Commit();
                }

                //-----------------------------------------------------------------------------------------

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    //string docTable = DocumentsStorageTablesPrefix + "d" + docSpaceIdx.ToString();
                    syncroTables.Add(DocumentsStorageTablesPrefix + "p");
                    //Console.WriteLine("{0}> started add sync", DateTime.Now.ToString("mm:ss.ms"));
                    tran.SynchronizeTables(syncroTables.ToList());
                    //Console.WriteLine("{0}> ended add sync", DateTime.Now.ToString("mm:ss.ms"));

                    //I1 dhl = null;
                    InTran_DocumentAppender docAppender = new InTran_DocumentAppender(tran, this.DocumentsStorageTablesPrefix);
                    foreach (var doc in docs)
                    {
                        docAppender.AppendDocument(doc);                       
                    }//eo foreach

                    tran.Commit();
                }

                return docs;
            }
            catch (Exception ex)
            {
                throw ThrowException("AddDocuments", ex.ToString());
            }
        }

        ///// <summary>
        ///// Adds documents to the storage.
        ///// </summary>
        ///// <param name="doc"></param>
        ///// <returns>Can return empty list in case of misunderstandings, or the same list back with cleaned Searchables, Content and set InternalId</returns>
        //public List<Document> AddDocuments(List<Document> docs)
        //{
            
        //    try
        //    {
        //        if (docs == null)
        //            throw ThrowException("AddDocuments", "supplied document is null");

        //        if (docs.Count() == 0)
        //            return new List<Document>(); 
        

        //        HashSet<string> syncroTables = new HashSet<string>();

        //        using (var tran = DBreezeEngine.GetTransaction())
        //        {
        //            tran.SynchronizeTables(DocumentsStorageTablesPrefix + "m");
        //            var mt = tran.InsertTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
        //            long maxDocSpaceId = tran.Select<int, long>(DocumentsStorageTablesPrefix + "m", 2).Value;

        //            foreach (var doc in docs)
        //            {
        //                if (String.IsNullOrEmpty(doc.DocumentSpace))
        //                    throw ThrowException("AddDocuments", "supplied Document Space can't be empty");

        //                if (String.IsNullOrEmpty(doc.Searchables))
        //                {
        //                    //throw ThrowException("AddDocuments", "Document Searchables can't be empty");
        //                }
        //                else if (doc.Searchables.Length > 50000000)
        //                    throw ThrowException("AddDocuments", "Document Searchables is bigger then 50MLN symbols"); //Can be changed, if necessary

        //                doc.DocumentSpaceId = mt.Select<string, long>(doc.DocumentSpace).Value;

        //                if (doc.DocumentSpaceId == 0)
        //                {
        //                    //Creating document space
        //                    maxDocSpaceId++;
        //                    doc.DocumentSpaceId = maxDocSpaceId;
        //                    tran.Insert<int, long>(DocumentsStorageTablesPrefix + "m", 2, maxDocSpaceId);
        //                    mt.Insert<string, long>(doc.DocumentSpace, doc.DocumentSpaceId);
        //                }

        //                if (!syncroTables.Contains(DocumentsStorageTablesPrefix + "d" + doc.DocumentSpaceId.ToString()))
        //                {
        //                    syncroTables.Add(DocumentsStorageTablesPrefix + "d" + doc.DocumentSpaceId.ToString());
        //                }
        //            }

        //            tran.Commit();
        //        }
                
        //        //-----------------------------------------------------------------------------------------
               
        //        //Document space Id, help tables for storing nested tables
        //        Dictionary<long, I1> h = new Dictionary<long, I1>();
        //        byte[] serDoc = null;
        //        byte[] btDoc = null;

        //        using (var tran = DBreezeEngine.GetTransaction())
        //        {
        //            //string docTable = DocumentsStorageTablesPrefix + "d" + docSpaceIdx.ToString();
        //            syncroTables.Add(DocumentsStorageTablesPrefix + "p");
        //            //Console.WriteLine("{0}> started add sync", DateTime.Now.ToString("mm:ss.ms"));
        //            tran.SynchronizeTables(syncroTables.ToList());
        //            //Console.WriteLine("{0}> ended add sync", DateTime.Now.ToString("mm:ss.ms"));

        //            I1 dhl = null;
        //            foreach (var doc in docs)
        //            {
        //                if (!h.TryGetValue(doc.DocumentSpaceId, out dhl))
        //                {
        //                    dhl = new I1()
        //                    {
        //                        DocTableName = DocumentsStorageTablesPrefix + "d" + doc.DocumentSpaceId.ToString()
        //                    };

        //                    //document table
        //                    dhl.dt = tran.InsertTable<int>(dhl.DocTableName, 1, 0);
        //                    //Version table Key is composite InitialDocId(int)+VersionNumber(int)+SequentialDocId(int)
        //                    dhl.vt = tran.InsertTable<int>(dhl.DocTableName, 3, 0);
        //                    dhl.et = tran.InsertTable<int>(dhl.DocTableName, 2, 0); //ExternalId to InternalId relation
        //                    dhl.MaxDocId = tran.Select<int, int>(dhl.DocTableName, 4).Value;

        //                    h[doc.DocumentSpaceId] = dhl;
        //                }
                      

        //                //Increasing maximal docIndex in the docSpace
        //                dhl.MaxDocId++;
        //                tran.Insert<int, int>(dhl.DocTableName, 4, dhl.MaxDocId);

        //                //Saving doc content separately and repack instead SelectDirect link
        //                if (doc.Content != null)
        //                {
        //                    //16 bytes link to Content
        //                    doc.Content = dhl.dt.InsertDataBlock(null, doc.Content);
        //                }
                        
        //                //Extra compressing searchables routine.
        //                if (!String.IsNullOrEmpty(doc.Searchables))
        //                {
        //                    byte[] btSearchables = System.Text.Encoding.UTF8.GetBytes(doc.Searchables);
        //                    byte[] btSearchablesZipped = btSearchables.CompressGZip();
        //                    if (btSearchablesZipped.Length < btSearchables.Length)
        //                        btSearchables = new byte[] { 1 }.Concat(btSearchablesZipped);
        //                    else
        //                        btSearchables = new byte[] { 0 }.Concat(btSearchables);

        //                    doc.InternalStructure = dhl.dt.InsertDataBlock(null, btSearchables);

        //                    //Now document is lightweight, without real content and searchables
        //                    doc.Searchables = null;
        //                }
                       

        //                if (!String.IsNullOrEmpty(doc.ExternalId))
        //                {
        //                    //If externalID is supplied, we use it to retrieve internal id
        //                    doc.InternalId = dhl.et.Select<string, int>(doc.ExternalId).Value;

        //                    if (doc.InternalId == 0)
        //                    {
        //                        //New doc
        //                        doc.DocumentSequentialId = dhl.MaxDocId;
        //                        doc.InternalId = doc.DocumentSequentialId;
        //                        dhl.et.Insert<string, int>(doc.ExternalId, doc.InternalId);                                                                
        //                        //Inserting into version table 
        //                        //Console.WriteLine("Adding_V_" + doc.InternalId + "_1_" + doc.DocumentSequentialId);
        //                        dhl.vt.Insert<byte[], byte[]>(doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(((int)1).To_4_bytes_array_BigEndian(), doc.DocumentSequentialId.To_4_bytes_array_BigEndian()), new byte[] { 0 });
        //                    }
        //                    else
        //                    {
        //                        //Updating document (we create new version)
        //                        doc.DocumentSequentialId = dhl.MaxDocId;                                
        //                        //Getting version number
        //                        foreach (var row in dhl.vt.SelectBackwardFromTo<byte[], byte>(
        //                            doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MaxValue.To_4_bytes_array_BigEndian(), int.MaxValue.To_4_bytes_array_BigEndian()), true, 
        //                            doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MinValue.To_4_bytes_array_BigEndian(), int.MinValue.To_4_bytes_array_BigEndian()), true
        //                            ))
        //                        {
        //                            //Inserting into version table, new version
        //                            //Console.WriteLine("Adding_V_" + doc.InternalId + "_" + (row.Key.Substring(4, 4).To_Int32_BigEndian() + 1) + "_" + doc.DocumentSequentialId);
        //                            dhl.vt.Insert<byte[], byte[]>(doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany
        //                                (
        //                                    (row.Key.Substring(4, 4).To_Int32_BigEndian() + 1).To_4_bytes_array_BigEndian(),
        //                                    doc.DocumentSequentialId.To_4_bytes_array_BigEndian()
        //                                ), new byte[] {0});
        //                            break;
        //                        }

        //                    }
        //                }
        //                else
        //                {
        //                    if (doc.InternalId < 1)
        //                    {
        //                        //New doc
        //                        doc.DocumentSequentialId = dhl.MaxDocId;
        //                        doc.InternalId = doc.DocumentSequentialId;                                
        //                        //Inserting into version table
        //                        //Console.WriteLine("Adding_V_" + doc.InternalId + "_1_" + doc.DocumentSequentialId);
        //                        dhl.vt.Insert<byte[], byte[]>(doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(((int)1).To_4_bytes_array_BigEndian(), doc.DocumentSequentialId.To_4_bytes_array_BigEndian()), new byte[] {0});
        //                    }
        //                    else
        //                    {
        //                        //Updating document (we create new version)
        //                        doc.DocumentSequentialId = dhl.MaxDocId;                                
        //                        //Getting version number
        //                        foreach (var row in 
        //                            dhl.vt.SelectBackwardFromTo<byte[], byte>(
        //                            doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MaxValue.To_4_bytes_array_BigEndian(), int.MaxValue.To_4_bytes_array_BigEndian()), true,
        //                            doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MinValue.To_4_bytes_array_BigEndian(), int.MinValue.To_4_bytes_array_BigEndian()), true
        //                            ))
        //                        {
        //                            //Inserting into version table, new version
        //                            //Console.WriteLine("Adding_V_" + doc.InternalId + "_" + (row.Key.Substring(4, 4).To_Int32_BigEndian() + 1) + "_" + doc.DocumentSequentialId);
        //                            dhl.vt.Insert<byte[], byte[]>(doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany
        //                                (
        //                                    (row.Key.Substring(4, 4).To_Int32_BigEndian() + 1).To_4_bytes_array_BigEndian(),
        //                                    doc.DocumentSequentialId.To_4_bytes_array_BigEndian()
        //                                ), new byte[] {0});
        //                            break;
        //                        }
        //                    }
        //                }

        //                serDoc = doc.SerializeProtobuf();
        //                btDoc = serDoc.CompressGZip();

        //                if (serDoc.Length >= btDoc.Length)
        //                    btDoc = new byte[] { 1 }.Concat(btDoc);
        //                else
        //                    btDoc = new byte[] { 0 }.Concat(serDoc);
        //                dhl.dt.Insert<int, byte[]>(doc.DocumentSequentialId, btDoc);

        //                //-----------------------------------------------------------------------------------------

        //                //Adding to processing table
        //                //Console.WriteLine("Adding_P_" + doc.DocumentSequentialId);
        //                tran.Insert<byte[], byte>(DocumentsStorageTablesPrefix + "p", doc.DocumentSpaceId.To_8_bytes_array_BigEndian().Concat(doc.DocumentSequentialId.To_4_bytes_array_BigEndian()), 0);

        //            }//eo foreach
                   
        //            tran.Commit();
        //        }

        //        return docs;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ThrowException("AddDocuments", ex.ToString());
        //    }
        //}

        /// <summary>
        /// <para>Activating of documents indexing.</para>  
        /// <para>Runs in a separate thread and doesn't block calling thread.</para>
        /// <para>Must be called right after AddDocuments routine</para>
        /// </summary>
        public void StartDocumentsIndexing()
        {
            lock (lock_inProcessDocs)
            {
                if (InProcessDocs)
                {
                    //Console.WriteLine("Inproc");
                    return;
                }
            }

            System.Threading.Thread tr = new System.Threading.Thread(new System.Threading.ThreadStart(ProcessDocsAtOnce));
            tr.Start();            
        }

        /// <summary>
        /// Technical class
        /// </summary>
        class I2
        {
            public byte[] PReference = null;
            /// <summary>
            /// Can be null
            /// </summary>
            public Document doc = null;
            /// <summary>
            /// Flag (if not null) indicates that we must remove document from the search and clean VersionTable
            /// </summary>
            public byte[] VersionDocumentToRemove = null;
        }

        bool InProcessDocs = false;
        object lock_inProcessDocs = new object();

        /// <summary>
        /// Indicates that system is in document processing mode
        /// </summary>
        public bool IsProcessing
        {
            get {
                lock (lock_inProcessDocs)
                {
                    return InProcessDocs;
                }
            }
        }
        
      
        /// <summary>
        /// 
        /// </summary>
        void ProcessDocsAtOnce()
        {
            lock (lock_inProcessDocs)
            {
                if (InProcessDocs)
                    return;
                InProcessDocs = true;
            }

            Verbose("Processing has started.");

            OnProcessingStarted();

            List<I2> docsToBeProcessed = new List<I2>();

            try
            {
                long docSpaceId = 0;
                DBreeze.DataTypes.NestedTable dt = null;
                DBreeze.DataTypes.NestedTable vt = null;    //Version table
                string DocTableName = "";
                byte[] btDoc = null;
                Document doc = null;
                Document docVersion = null;                
                int ProcessedChars = 0;
                byte[] documentVersionToRemove = null;
                byte[] btSearchanbles = null;

                //Getting documents to be processed                 

                while (true)        //Avoiding flexing of the stack
                {
                    if (dt != null)
                    {
                        dt.Dispose();
                        dt = null;
                    }

                    if (vt != null)
                    {
                        vt.Dispose();
                        vt = null;
                    }

                    using (var tran = DBreezeEngine.GetTransaction())
                    {                       
                        //Console.WriteLine("P has " + tran.Count(DocumentsStorageTablesPrefix + "p"));

                        tran.ValuesLazyLoadingIsOn = false;

                        ProcessedChars = 0;
                        docsToBeProcessed.Clear();

                        bool docMustBeDeleted = false;

                        foreach (var row in tran.SelectForward<byte[], byte>(DocumentsStorageTablesPrefix + "p").Take(10000))
                        {
                            if (ProcessedChars > MaxCharsToBeProcessedPerRound)
                            {
                                //We must start procedure of saving
                                break;
                            }

                            docMustBeDeleted = false;

                            if (row.Value == 1)   //It means that this document version must be marked as deleted, 0 - inserting
                                docMustBeDeleted = true;

                            var thisDocSpaceId = row.Key.Substring(0, 8).To_Int64_BigEndian();
                            var thisDocId = row.Key.Substring(8, 4).To_Int32_BigEndian();

                            if (docSpaceId > 0 && docSpaceId != thisDocSpaceId)
                            {
                                //This is not that docSpaceId we have started here
                                continue;
                            }
                            else
                            {
                                //Collecting documents to be processed
                                docSpaceId = thisDocSpaceId;

                                //Reading document
                                if (dt == null)
                                {
                                    //reading first time
                                    DocTableName = DocumentsStorageTablesPrefix + "d" + thisDocSpaceId.ToString();
                                    dt = tran.SelectTable<int>(DocTableName, 1, 0);
                                    vt = tran.SelectTable<int>(DocTableName, 3, 0);
                                    vt.ValuesLazyLoadingIsOn = false;
                                }

                                var rowbtDoc = dt.Select<int, byte[]>(thisDocId);
                                if (!rowbtDoc.Exists)
                                {
                                    //We can't retrieve document, just skip and must be deleted in ProcessDocsBlock
                                    docsToBeProcessed.Add(new I2() { PReference = row.Key });
                                    continue;
                                }

                                btDoc = rowbtDoc.Value;

                                if (btDoc[0] == 0)
                                    doc = btDoc.Substring(1).DeserializeProtobuf<Document>();
                                else
                                    doc = btDoc.Substring(1).DecompressGZip().DeserializeProtobuf<Document>();

                                documentVersionToRemove = null;
                                                              
                                //Iterating through versions, marking old versions as deleted and de-indexing them, indexing latest version
                                foreach (var versionRow in
                                    vt.SelectForwardFromTo<byte[], byte[]>(
                                        doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MinValue.To_4_bytes_array_BigEndian(), int.MinValue.To_4_bytes_array_BigEndian()), true,
                                        doc.InternalId.To_4_bytes_array_BigEndian().ConcatMany(int.MaxValue.To_4_bytes_array_BigEndian(), int.MaxValue.To_4_bytes_array_BigEndian()), true)
                                        )
                                {
                                    if (versionRow.Value[0] == 1)   //This version is already deleted
                                        continue;

                                    rowbtDoc = dt.Select<int, byte[]>(versionRow.Key.Substring(8, 4).To_Int32_BigEndian());
                                    //It must exist
                                    btDoc = rowbtDoc.Value;

                                    if (btDoc[0] == 0)
                                        docVersion = btDoc.Substring(1).DeserializeProtobuf<Document>();
                                    else
                                        docVersion = btDoc.Substring(1).DecompressGZip().DeserializeProtobuf<Document>();

                                    if (docVersion.InternalStructure != null)  //In case if this document has searchables inside
                                    {
                                        btSearchanbles = dt.SelectDataBlock(docVersion.InternalStructure);

                                        //Getting searchables
                                        if (btSearchanbles[0] == 0)
                                        {
                                            //Not compressed
                                            docVersion.Searchables = System.Text.Encoding.UTF8.GetString(btSearchanbles.Substring(1));
                                        }
                                        else
                                        {
                                            //Compressed
                                            docVersion.Searchables = System.Text.Encoding.UTF8.GetString(btSearchanbles.Substring(1).DecompressGZip());
                                        }

                                        ProcessedChars += docVersion.Searchables.Length;
                                    }

                                    documentVersionToRemove = null;

                                    if (docMustBeDeleted || versionRow.Key.Substring(8, 4).To_Int32_BigEndian() != doc.DocumentSequentialId)
                                    {
                                        //Version of the document must be marked as deleted
                                        documentVersionToRemove = versionRow.Key;
                                    }

                                    //inserting into docsToBeProcessed
                                    docsToBeProcessed.Add(new I2() { PReference = docSpaceId.To_8_bytes_array_BigEndian().Concat(versionRow.Key.Substring(8, 4)), doc = docVersion, VersionDocumentToRemove = documentVersionToRemove });
                                }

                            }
                        }//eo foreach

                    }//end of using

                    lock (lock_inProcessDocs)
                    {
                        if (docsToBeProcessed.Count() == 0)
                        {
                            InProcessDocs = false;
                            Verbose("Processing has finished.");
                            this.OnProcessingStopped();
                            return;
                        }
                    }

                    ProcessDocsBlock(docsToBeProcessed);

                }  //end of while true                

              
            }
            catch (Exception ex)
            {

            }

           
        }

        /// <summary>
        /// ...
        /// </summary>
        class WordInDoc
        {
            /// <summary>
            /// Docs which contain this word
            /// </summary>
            public HashSet<int> docsAdded = new HashSet<int>();
            /// <summary>
            /// Docs which must be removed from word WAH
            /// </summary>
            public HashSet<int> docsRemoved = new HashSet<int>();
            public int BlockId = 0;
            public int NumberInBlock = 0;
            public bool ExistsInDb = false;
            public int foundOrigin = 0;
            public WAH2 wah = null;
            public int blockLength = 0;
        }



      

        /// <summary>
        /// 
        /// </summary>
        void ProcessDocsBlock(List<I2> i2s)
        {
            try
            {
                Verbose("Block processing is in progress...");                           
                System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                sw.Start();

                //Console.WriteLine("ProcessDocsBlock started with {0} elements", i2s.Count());

                System.Diagnostics.Stopwatch swSelect = new System.Diagnostics.Stopwatch();
                System.Diagnostics.Stopwatch swInsert = new System.Diagnostics.Stopwatch();
                                
                WAH2 wah = null;
                byte[] val = null;
                int uniqueWordsFound = 0;

                WordInDoc wd = null;
                Dictionary<string, WordInDoc> wds = new Dictionary<string, WordInDoc>();

                List<byte[]> PRemoves = new List<byte[]>();
                List<byte[]> VTRemoves = new List<byte[]>();
                

                using (var tran = DBreezeEngine.GetTransaction())
                {                    
                    string searchTable = DocumentsStorageTablesPrefix + "s" + i2s.First().doc.DocumentSpaceId.ToString();

                    tran.SynchronizeTables(searchTable);
                    
                    //Setting WAH index table
                    var tbOneWordWAH = tran.InsertTable<int>(searchTable, 2, 0);
                    tbOneWordWAH.ValuesLazyLoadingIsOn = false;
                    tbOneWordWAH.Technical_SetTable_OverwriteIsNotAllowed();

                    //Nested table with blocks
                    var tbBlocks = tran.InsertTable<int>(searchTable, 10, 0);   //Overwrite is needed
                    tbBlocks.ValuesLazyLoadingIsOn = false;


                    int currentBlock = tran.Select<int, int>(searchTable, 11).Value;
                    int numberInBlock = tran.Select<int, int>(searchTable, 12).Value;

                    if (currentBlock == 0)
                    {
                        numberInBlock = 0;
                        currentBlock = 1;
                    }

                    bool DocumentIsAdded = true;
                    
                    foreach (var i2 in i2s)
                    {
                        //Removing from "p"
                        PRemoves.Add(i2.PReference);
                     
                        //Removing from Version table
                        DocumentIsAdded = true;
                        if (i2.VersionDocumentToRemove != null)
                        {
                            VTRemoves.Add(i2.VersionDocumentToRemove);                            
                            DocumentIsAdded = false;
                        }

                        //doc can be null, doc can be without searchables
                        if (i2.doc == null || String.IsNullOrEmpty(i2.doc.Searchables))
                            continue;
                        
                        var wordsCounter = GetWordsDefinitionFromText(i2.doc.Searchables);

                        foreach (var el in wordsCounter.OrderBy(r => r.Key))
                        {
                            //Trying to get from Dictionary
                            if (!wds.TryGetValue(el.Key, out wd))
                            {
                                //getting from db
                                swSelect.Start();
                                var row1 = tbOneWordWAH.Select<string, byte[]>(el.Key, true);
                                swSelect.Stop();

                                if (row1.Exists)
                                {
                                    val = row1.Value;

                                    wd = new WordInDoc()
                                    {                                        
                                        BlockId = val.Substring(0,4).To_Int32_BigEndian(),
                                        NumberInBlock = val.Substring(4, 4).To_Int32_BigEndian(),
                                        ExistsInDb = true   //We don't need to save this word again (only its WAH in block)
                                        
                                    };                                  
                                }
                                else
                                {
                                    numberInBlock++;

                                    if (numberInBlock > QuantityOfWordsInBlock)  //Quantity of words (WAHs) in block
                                    {
                                        currentBlock++;
                                        numberInBlock = 1;
                                    }

                                    wd = new WordInDoc()
                                    {   
                                        BlockId = currentBlock,
                                        NumberInBlock = numberInBlock,
                                    };

                                   // Console.WriteLine(el.Key + " " + wd.NumberInBlock);

                                    uniqueWordsFound++;
                                }
                            }
                          
                            //Adding to wah document id
                            if (DocumentIsAdded)
                            {
                                if (!wd.docsAdded.Contains(i2.doc.DocumentSequentialId))
                                    wd.docsAdded.Add(i2.doc.DocumentSequentialId);
                            }
                            else
                            {
                                if (!wd.docsRemoved.Contains(i2.doc.DocumentSequentialId))
                                    wd.docsRemoved.Add(i2.doc.DocumentSequentialId);
                            }

                            //Applying it to the memory wah storage
                            wds[el.Key] = wd;

                        }//eo foreach words in document


                    }//eo foreach documnent

                    
                    //Inserting new words
                    foreach (var wd1 in wds.OrderBy(r => r.Key))
                    {
                        if (!wd1.Value.ExistsInDb)
                        {
                            swInsert.Start();
                            //Console.WriteLine("{0} {1}", wd1.Key, wd1.Value.NumberInBlock);
                            tbOneWordWAH.Insert<string, byte[]>(wd1.Key, wd1.Value.BlockId.To_4_bytes_array_BigEndian().Concat(wd1.Value.NumberInBlock.To_4_bytes_array_BigEndian()));
                            swInsert.Stop();
                        }
                    }
                    
                    //Inserting WAH blocks
                    //Going through the list of collected words order by blockID, fill blocks and save them
                    int iterBlockId = 0;
                    int iterBlockLen = 0;
                    int blockSize = 0;
                    byte[] btBlock = null;
                    Dictionary<int, byte[]> block = new Dictionary<int, byte[]>();
                    byte[] btWah = null;
                    byte[] tmp = null;

                    foreach (var wd1 in wds.OrderBy(r => r.Value.BlockId))
                    {

                        //reading block if it's not loaded
                        if (wd1.Value.BlockId != iterBlockId)
                        {
                            if (iterBlockId > 0)
                            {
                                //We must save current datablock
                                if (block.Count() > 0)
                                {                                   

                                    btBlock = block.SerializeProtobuf();
                                    btBlock = btBlock.CompressGZip();

                                   // Console.WriteLine("Block {0} Len {1}",iterBlockId, btBlock.Length);


                                    if ((btBlock.Length + 4) < MinimalBlockReservInBytes)    //Minimal reserv
                                    {
                                        tmp = new byte[MinimalBlockReservInBytes];
                                        tmp.CopyInside(0, btBlock.Length.To_4_bytes_array_BigEndian());
                                        tmp.CopyInside(4, btBlock);
                                    }
                                    else if ((btBlock.Length + 4) > iterBlockLen)
                                    {
                                        //Doubling reserve
                                        tmp = new byte[btBlock.Length * 2];
                                        tmp.CopyInside(0, btBlock.Length.To_4_bytes_array_BigEndian());
                                        tmp.CopyInside(4, btBlock);
                                    }
                                    else
                                    {
                                        //Filling existing space
                                        tmp = new byte[btBlock.Length + 4];
                                        tmp.CopyInside(0, btBlock.Length.To_4_bytes_array_BigEndian());
                                        tmp.CopyInside(4, btBlock);
                                    }

                                    //Saving into DB
                                    swInsert.Start();
                                    tbBlocks.Insert<int, byte[]>(iterBlockId, tmp);
                                    swInsert.Stop();
                                }

                                block = null;
                            }

                            val = tbBlocks.Select<int, byte[]>(wd1.Value.BlockId).Value;
                            iterBlockId = wd1.Value.BlockId;
                            iterBlockLen = val == null ? 0 : val.Length;

                            if (val != null)
                            {
                                blockSize = val.Substring(0, 4).To_Int32_BigEndian();
                                if (blockSize > 0)
                                {
                                    btBlock = val.Substring(4, blockSize);
                                    btBlock = btBlock.DecompressGZip();
                                    block = btBlock.DeserializeProtobuf<Dictionary<int, byte[]>>();
                                }
                                else
                                    block = new Dictionary<int, byte[]>();
                            }
                            else
                                block = new Dictionary<int, byte[]>();
                        }

                        //Getting from Block 
                        if (block.TryGetValue(wd1.Value.NumberInBlock, out btWah))
                        {
                            wah = new WAH2(btWah);
                        }
                        else
                            wah = new WAH2(null);

                        //Adding documents
                        foreach (var d in wd1.Value.docsAdded)
                            wah.Add(d, true);

                        //Removing documents
                        foreach (var d in wd1.Value.docsRemoved)
                            wah.Add(d, false);

                        block[wd1.Value.NumberInBlock] = wah.GetCompressedByteArray();

                    }//eo foreach


                    //Saving last element
                    if (block != null)
                    {
                        //saving current block
                        if (block.Count() > 0)
                        {
                            //!!!!!!!!!!! Remake it for smoothing storage 
                            btBlock = block.SerializeProtobuf();
                            btBlock = btBlock.CompressGZip();

                            if ((btBlock.Length + 4) < MinimalBlockReservInBytes)    //Minimal reserve
                            {
                                tmp = new byte[MinimalBlockReservInBytes];
                                tmp.CopyInside(0, btBlock.Length.To_4_bytes_array_BigEndian());
                                tmp.CopyInside(4, btBlock);
                            }
                            else if ((btBlock.Length + 4) > iterBlockLen)
                            {
                                //Doubling reserve
                                tmp = new byte[btBlock.Length * 2];
                                tmp.CopyInside(0, btBlock.Length.To_4_bytes_array_BigEndian());
                                tmp.CopyInside(4, btBlock);
                            }
                            else
                            {
                                //Filling existing space
                                tmp = new byte[btBlock.Length + 4];
                                tmp.CopyInside(0, btBlock.Length.To_4_bytes_array_BigEndian());
                                tmp.CopyInside(4, btBlock);
                            }                        

                            //Saving into DB
                            swInsert.Start();
                            tbBlocks.Insert<int, byte[]>(iterBlockId, tmp);
                            swInsert.Stop();
                        }

                        block = null;
                    }

                    tran.Insert<int, int>(searchTable, 11,currentBlock);
                    tran.Insert<int, int>(searchTable, 12, numberInBlock);
                    
                    tran.Commit();
                }//eo tran


                //Moved away P and VT tables Removes to avoid suspending of AddDocuments thread
                if(PRemoves.Count()>0 || VTRemoves.Count()>0)
                {
                    swInsert.Start();
                    using (var tran = DBreezeEngine.GetTransaction())
                    {
                        string docTable = DocumentsStorageTablesPrefix + "d" + i2s.First().doc.DocumentSpaceId.ToString();
                        //Console.WriteLine("{0}> started process sync", DateTime.Now.ToString("mm:ss.ms"));
                        tran.SynchronizeTables(docTable, DocumentsStorageTablesPrefix + "p");
                        //Console.WriteLine("{0}> ended process sync", DateTime.Now.ToString("mm:ss.ms"));
                        
                        var vt = tran.InsertTable<int>(docTable, 3, 0);

                        foreach (var el in VTRemoves)
                        {
                            //Console.WriteLine("Updating_V_" + el.Substring(0, 4).To_Int32_BigEndian() + "_" + el.Substring(4, 4).To_Int32_BigEndian() + "_" + el.Substring(8, 4).To_Int32_BigEndian());
                            //Setting version as deleted from keyword blocks
                            vt.Insert<byte[], byte[]>(el, new byte[] { 1 });                           
                        }

                        foreach (var el in PRemoves)
                        {
                            //Console.WriteLine("Removing_P_" + el.Substring(8, 4).To_Int32_BigEndian());
                            tran.RemoveKey<byte[]>(DocumentsStorageTablesPrefix + "p", el);
                        }
                        
                        tran.Commit();
                    }
                    swInsert.Stop();
                }

                sw.Stop();

                Verbose("Processed {0} documents with {3} words in DocuSpace {1}. Took {2} ms; Select {4} ms; Insert {5} ms; UniqueWords: {6}", i2s.Count(), i2s.First().doc.DocumentSpaceId, sw.ElapsedMilliseconds, wds.Count(), swSelect.ElapsedMilliseconds, swInsert.ElapsedMilliseconds, uniqueWordsFound);

            }
            catch (Exception ex)
            {
              //  throw ThrowException("ProcessDocsBlock", ex.ToString());
            }
            finally
            {

            }

           // Console.WriteLine("ProcessDocsBlock finished");
       
        }

        /// <summary>
        /// Initializes useful console ouputs if true
        /// </summary>
        public bool VerboseConsoleEnabled = false;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        void Verbose(string format, params object[] args)
        {
            if (VerboseConsoleEnabled)
            {
                Console.Write("DBreezeBased.DocumentStorage. ");
                Console.WriteLine(format, args);
            }
        }
               

        [ProtoBuf.ProtoContract]
        class Word
        {
            [ProtoBuf.ProtoMember(1, IsRequired = true)]
            public byte[] WAH2 { get; set; }
        }
        
        /// <summary>
        /// 
        /// </summary>        
        class WordDefinition
        {            
            public uint CountInDocu = 0;
        }

        /// <summary>
        /// Returns null in case of notfound anything or what ever
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        Dictionary<string, WordDefinition> GetWordsDefinitionFromText(string text)
        {
            try
            {
                if (String.IsNullOrEmpty(text))
                    return null;

                StringBuilder sb = new StringBuilder();
                string word = "";
                WordDefinition wordDefinition = null;
                Dictionary<string, WordDefinition> wordsCounter = new Dictionary<string, WordDefinition>();

                Action processWord = () =>
                {
                    //We take all words, so we can later find even by email address jj@gmx.net ... we will need jj and gmx.net
                    if (sb.Length > 0)
                    {
                        word = sb.ToString().ToLower();
                        
                        List<string> wrds = new List<string>();
                        wrds.Add(word);
                        int i=1;

                        

                        if (this.SearchWordMinimalLength > 0)   //If equals to 0, we store only words for full text search
                        {
                            while (word.Length - i >= this.SearchWordMinimalLength)
                            {
                                wrds.Add(word.Substring(i));
                                i++;
                            }
                        }

                        foreach (var w in wrds)
                        {
                            if (wordsCounter.TryGetValue(w, out wordDefinition))
                            {
                                wordDefinition.CountInDocu++;
                            }
                            else
                            {
                                wordDefinition = new WordDefinition() { CountInDocu = 1 };
                                wordsCounter[w] = wordDefinition;
                            }
                        }
                        
                    }

                    if(sb.Length>0)
                        sb.Remove(0, sb.Length);
                    //sb.Clear();
                };

                int wordLen = 0;
                int maximalWordLengthBeforeSplit = 50;

                foreach (var c in text)
                {
                    if (c=='-' || c=='@')   //Complex names or email address inside
                        continue;

                    if (Char.IsLetterOrDigit(c) || Char.IsSymbol(c))
                    {
                        sb.Append(c);
                        wordLen++;

                        if (wordLen >= maximalWordLengthBeforeSplit)
                        {
                            //Processing ready word
                            processWord();
                            wordLen = 0;
                        }
                    }
                    else
                    {
                        //Processing ready word
                        processWord();
                        wordLen = 0;
                    }
                }

                //Processing last word
                processWord();

                if (wordsCounter.Count() > 0)
                    return wordsCounter;
            }
            catch (System.Exception ex)
            {
              
            }

            return null;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="includeContent"></param>
        /// <param name="includeSearchables"></param>
        /// <param name="dt"></param>
        /// <param name="docRow"></param>
        /// <returns></returns>
        Document RetrieveDocument(bool includeContent, bool includeSearchables, DBreeze.DataTypes.NestedTable dt, DBreeze.DataTypes.Row<int,byte[]> docRow)
        {

            byte[] btDoc = docRow.Value;
            Document doc = null;

            if (btDoc[0] == 0)
            {
                //Non compressed
                doc = btDoc.Substring(1).DeserializeProtobuf<Document>();
            }
            else
            {
                doc = btDoc.Substring(1).DecompressGZip().DeserializeProtobuf<Document>();
            }

            if (includeContent && doc.Content != null)
            {
                //16 bytes link to Content
                doc.Content = dt.SelectDataBlock(doc.Content);
            }

            if (includeSearchables && doc.InternalStructure != null)
            {
                byte[] btSearchables = dt.SelectDataBlock(doc.InternalStructure);
                if (btSearchables[0] == 1)
                {
                    //Zipped
                    btSearchables = btSearchables.Substring(1).DecompressGZip();
                }
                else
                    btSearchables = btSearchables.Substring(1);

                doc.Searchables = System.Text.Encoding.UTF8.GetString(btSearchables);
            }

            doc.DocumentSequentialId = docRow.Key;
            return doc;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="docSpaceId"></param>
        /// <param name="internalId"></param>
        /// <param name="includeContent"></param>
        /// <param name="includeSearchables"></param>
        /// <param name="tran"></param>
        /// <returns></returns>
        Document GetDocById(long docSpaceId, int internalId, bool includeContent, bool includeSearchables, DBreeze.Transactions.Transaction tran)
        {
            try
            {
                if (docSpaceId < 1 || internalId < 1)
                        return null;    //No such document

                string docTable = DocumentsStorageTablesPrefix + "d" + docSpaceId.ToString();

                //Getting latest document correct version
                string DocTableName = DocumentsStorageTablesPrefix + "d" + docSpaceId.ToString();
                var vt = tran.SelectTable<int>(DocTableName, 3, 0);
                int sequentialId = 0;

                foreach (var row1 in vt.SelectBackwardFromTo<byte[], byte>(
                                        internalId.To_4_bytes_array_BigEndian().ConcatMany(int.MaxValue.To_4_bytes_array_BigEndian(), int.MaxValue.To_4_bytes_array_BigEndian()), true,
                                        internalId.To_4_bytes_array_BigEndian().ConcatMany(int.MinValue.To_4_bytes_array_BigEndian(), int.MinValue.To_4_bytes_array_BigEndian()), true                                        
                                        ))
                {
                    if (row1.Value == 1)    //Document is completely deleted
                        break;

                    //Getting the latest version of the document index
                    sequentialId = row1.Key.Substring(8, 4).To_Int32_BigEndian();

                    break;
                }

                if (sequentialId == 0)
                    return null;    //Probably Doc is deleted

                //Getting document using internalID
                var dt = tran.SelectTable<int>(docTable, 1, 0); //document table

                dt.ValuesLazyLoadingIsOn = false;
                var row = dt.Select<int, byte[]>(sequentialId);
                if (!row.Exists)
                    return null;    //No such document

                return RetrieveDocument(includeContent, includeSearchables, dt, row);               
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        /// <summary>
        /// Function to be used inside of internal transaction
        /// </summary>
        /// <param name="tran"></param>
        /// <param name="documentSpace"></param>
        /// <returns></returns>
        public long InTran_GetDocumentSpaceId(DBreeze.Transactions.Transaction tran, string documentSpace)
        {
            var mt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
            var docSpaceId = mt.Select<string, long>(documentSpace).Value;

            return docSpaceId;
        }
        
        /// <summary>
        /// Returns DocumentSpaceId (internally opens new transaction)
        /// </summary>
        /// <param name="documentSpace"></param>
        /// <param name="createIfNotFound"></param>
        /// <returns></returns>
        public long GetDocumentSpaceId(string documentSpace, bool createIfNotFound)
        {
            try
            {
                if (String.IsNullOrEmpty(documentSpace))
                    return 0;    //Wrong parameters

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    tran.SynchronizeTables(DocumentsStorageTablesPrefix + "m");
                    //Getting document space index
                    var mt = tran.InsertTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
                    var docSpaceId = mt.Select<string, long>(documentSpace).Value;

                    if (docSpaceId == 0 && createIfNotFound)
                    {
                        long maxDocSpaceId = tran.Select<int, long>(DocumentsStorageTablesPrefix + "m", 2).Value;
                        maxDocSpaceId++;                        
                        tran.Insert<int, long>(DocumentsStorageTablesPrefix + "m", 2, maxDocSpaceId);
                        mt.Insert<string, long>(documentSpace, maxDocSpaceId);
                        tran.Commit();
                        return maxDocSpaceId;
                    }

                    return docSpaceId;                    
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        
        /// <summary>
        /// Can return null, if not found.
        ///  (internally opens new transaction)
        /// </summary>
        /// <param name="documentSpace"></param>
        /// <param name="externalId"></param>
        /// <param name="includeContent"></param>
        /// <param name="includeSearchables"></param>
        /// <returns></returns>
        public Document GetDocumentByExternalID(string documentSpace, string externalId, bool includeContent, bool includeSearchables)
        {
            try
            {
                if (String.IsNullOrEmpty(documentSpace) || String.IsNullOrEmpty(externalId))
                    return null;    //Wrong parameters

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    //Getting document space index
                    var mt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
                    var docSpaceId = mt.Select<string, long>(documentSpace).Value;

                    if (docSpaceId == 0)
                        return null;    //No such document space

                    //Getting internalID through External ID
                    string docTable = DocumentsStorageTablesPrefix + "d" + docSpaceId.ToString();
                    var et = tran.SelectTable<int>(docTable, 2, 0); //ExternalId to InternalId relation
                    int internalId = et.Select<string, int>(externalId).Value;

                    if (internalId == 0)
                        return null;    //No such document

                    return GetDocById(docSpaceId, internalId, includeContent, includeSearchables, tran);
                }
            }
            catch (Exception ex)
            {
                throw ex;  
            }
           
        }

        /// <summary>
        ///  Can return null, if not found
        /// </summary>
        /// <param name="internalID"></param>
        /// <returns></returns>
        public Document GetDocumentByInternalID(string documentSpace, int internalId, bool includeContent, bool includeSearchables)
        {
            try
            {
                if (String.IsNullOrEmpty(documentSpace) || internalId < 1)
                    return null;    //Wrong parameters

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    //Getting document space index
                    var mt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
                    var docSpaceId = mt.Select<string, long>(documentSpace).Value;

                    if (docSpaceId == 0)
                        return null;    //No such document space

                    return GetDocById(docSpaceId, internalId, includeContent, includeSearchables, tran);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
           
        }

        /// <summary>
        /// Returning list can contain null values
        /// </summary>
        /// <param name="documentSpace"></param>
        /// <param name="internalIDs"></param>
        /// <param name="includeContent"></param>
        /// <param name="includeSearchables"></param>
        /// <returns></returns>
        public IList<Document> GetDocumentsByInternalIDs(string documentSpace, IList<int> internalIDs, bool includeContent, bool includeSearchables)
        {
            List<Document> ret = new List<Document>();

            try
            {
                if (String.IsNullOrEmpty(documentSpace) || internalIDs.Count() < 1)
                    return null;    //Wrong parameters

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    //Getting document space index
                    var mt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
                    var docSpaceId = mt.Select<string, long>(documentSpace).Value;

                    if (docSpaceId == 0)
                        return null;    //No such document space
                    

                    foreach (var id in internalIDs)
                    {
                        if(id < 1)
                        {
                            ret.Add(null);
                            continue;
                        }

                        ret.Add(GetDocById(docSpaceId, id, includeContent, includeSearchables, tran));
                    }
                    
                    return ret;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }


        /// <summary>
        /// RemoveDocumentByExternalID
        /// </summary>
        /// <param name="externalID"></param>
        public void RemoveDocumentByExternalID(string documentSpace, string externalId)
        {
            try
            {
                if (String.IsNullOrEmpty(documentSpace) || String.IsNullOrEmpty(externalId))
                    return;    //Wrong parameters
                              

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    //Getting document space index
                    var mt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
                    var docSpaceId = mt.Select<string, long>(documentSpace).Value;

                    if (docSpaceId == 0)
                        return;    //No such document space

                    RemoveDocumentInternal(docSpaceId, 0, externalId, tran);

                    tran.Commit();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            //Starting document indexing
            StartDocumentsIndexing();
        }

        /// <summary>
        /// RemoveDocumentByInternalID
        /// </summary>
        /// <param name="internalID"></param>
        public void RemoveDocumentByInternalID(string documentSpace, int internalId)
        {
            try
            {
                if (String.IsNullOrEmpty(documentSpace) || internalId < 1)
                    return;    //Wrong parameters


                using (var tran = DBreezeEngine.GetTransaction())
                {
                    //Getting document space index
                    var mt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
                    var docSpaceId = mt.Select<string, long>(documentSpace).Value;

                    if (docSpaceId == 0)
                        return;    //No such document space

                    RemoveDocumentInternal(docSpaceId, internalId,"", tran);

                    tran.Commit();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            //Starting document indexing
            StartDocumentsIndexing();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="docSpaceId"></param>
        /// <param name="internalId"></param>
        /// <param name="tran"></param>
        void RemoveDocumentInternal(long docSpaceId, int internalId, string externalId, DBreeze.Transactions.Transaction tran)
        {
            string docTable = DocumentsStorageTablesPrefix + "d" + docSpaceId.ToString();
            tran.SynchronizeTables(docTable, DocumentsStorageTablesPrefix + "p");
            //Getting document using internalID
            var dt = tran.InsertTable<int>(docTable, 1, 0); //document table
            var vt = tran.InsertTable<int>(docTable, 3, 0); //Version table Key
            vt.ValuesLazyLoadingIsOn = false;

            if (!String.IsNullOrEmpty(externalId))  //Getting internalId via external
            {
                var et = tran.InsertTable<int>(docTable, 2, 0); //ExternalId to InternalId relation
                internalId = et.Select<string, int>(externalId).Value;
            }

            if (internalId == 0)
                return;    //No such document

            //Iterating through all versions of the document
            foreach (var vtRow in vt.SelectBackwardFromTo<byte[], byte>
               (
               internalId.To_4_bytes_array_BigEndian().ConcatMany(int.MaxValue.To_4_bytes_array_BigEndian(), int.MaxValue.To_4_bytes_array_BigEndian()), true,
               internalId.To_4_bytes_array_BigEndian().ConcatMany(int.MinValue.To_4_bytes_array_BigEndian(), int.MinValue.To_4_bytes_array_BigEndian()), true
               , true
               ))
            {
                if (vtRow.Value == 0)
                {
                    //Including the last one into processing list with the value 1 (to be deleted)
                    tran.Insert<byte[], byte>(DocumentsStorageTablesPrefix + "p", docSpaceId.To_8_bytes_array_BigEndian().Concat(vtRow.Key.Substring(8, 4)), 1);
                }
                break;
            }

        }


        /// <summary>
        /// Will be returned sequential id's of document
        /// </summary>
        /// <param name="documentSpace"></param>
        /// <param name="externalId"></param>
        /// <returns></returns>
        public List<int> GetListOfDocumentVersions(string documentSpace, string externalId)
        {
            try
            {
                List<int> res = new List<int>();

                if (String.IsNullOrEmpty(documentSpace) || String.IsNullOrEmpty(externalId))
                    return res;    //Wrong parameters

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    //Getting document space index
                    var mt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
                    var docSpaceId = mt.Select<string, long>(documentSpace).Value;

                    if (docSpaceId == 0)
                        return res;    //No such document space
                                 
                    string DocTableName = DocumentsStorageTablesPrefix + "d" + docSpaceId.ToString();

                    var et = tran.SelectTable<int>(DocTableName, 2, 0); //ExternalId to InternalId relation
                    int internalId = et.Select<string, int>(externalId).Value;

                    if (internalId == 0)
                        return res;    //No such document

                    GetListOfDocumentVersionsInternal(docSpaceId, internalId, tran, res);
                }

                return res;
            }
            catch (Exception ex)
            {                
                throw ex;
            }
        }

        /// <summary>
        /// Will be returned sequential id's of document
        /// </summary>
        /// <returns></returns>
        public List<int> GetListOfDocumentVersions(string documentSpace, int internalId)
        {
            try
            {
                List<int> res = new List<int>();

                if (String.IsNullOrEmpty(documentSpace) || internalId < 1)
                    return res;    //Wrong parameters

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    //Getting document space index
                    var mt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
                    var docSpaceId = mt.Select<string, long>(documentSpace).Value;

                    if (docSpaceId == 0)
                        return res;    //No such document space

                    GetListOfDocumentVersionsInternal(docSpaceId, internalId, tran, res);
                }

                return res;
            }
            catch (Exception ex)
            {                
                throw ex;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="internalId"></param>
        /// <returns></returns>
        void GetListOfDocumentVersionsInternal(long docSpaceId, int internalId, DBreeze.Transactions.Transaction tran, List<int> res)
        {
            string DocTableName = DocumentsStorageTablesPrefix + "d" + docSpaceId.ToString();
            var vt = tran.SelectTable<int>(DocTableName, 3, 0);

            foreach (var row in vt.SelectForwardFromTo<byte[], byte>(
                                    internalId.To_4_bytes_array_BigEndian().ConcatMany(int.MinValue.To_4_bytes_array_BigEndian(), int.MinValue.To_4_bytes_array_BigEndian()), true,
                                    internalId.To_4_bytes_array_BigEndian().ConcatMany(int.MaxValue.To_4_bytes_array_BigEndian(), int.MaxValue.To_4_bytes_array_BigEndian()), true                                    
                                    ))
                                {
                                   res.Add(row.Key.Substring(8,4).To_Int32_BigEndian());
                                }

                                      
        }


        /// <summary>
        /// Rollbacks document (marked with internalId) to its version (marked as sequentialId).
        /// Can restore even completely deleted document
        /// </summary>
        /// <param name="documentSpace"></param>
        /// <param name="internalId">internal number of the document group (all versions of one document)</param>
        /// <param name="sequentialId"></param>
        /// <returns></returns>
        public bool RollbackToVersion(string documentSpace, int internalId, int sequentialId)
        {

            try
            {
                if (String.IsNullOrEmpty(documentSpace) || sequentialId < 1)
                    return false;    //Wrong parameters

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    //Getting document space index
                    var mt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
                    var docSpaceId = mt.Select<string, long>(documentSpace).Value;

                    if (docSpaceId == 0)
                        return false;    //No such document space

                    string DocTableName = DocumentsStorageTablesPrefix + "d" + docSpaceId.ToString();
                    tran.SynchronizeTables(DocumentsStorageTablesPrefix + "p",DocTableName);
                                        
                    var vt = tran.InsertTable<int>(DocTableName, 3, 0);
                    vt.ValuesLazyLoadingIsOn = false;
                    var dt = tran.InsertTable<int>(DocTableName, 1, 0);
                    dt.ValuesLazyLoadingIsOn = false;

                    byte[] DocumentVersionToRestore = null;
                    int versionNumber = -1;
                    //Iterating through document versions, to determine that this sequential number belongs to this document group
                    foreach (var row in vt.SelectBackwardFromTo<byte[], byte>(
                                      internalId.To_4_bytes_array_BigEndian().ConcatMany(int.MaxValue.To_4_bytes_array_BigEndian(), int.MaxValue.To_4_bytes_array_BigEndian()), true,
                                      internalId.To_4_bytes_array_BigEndian().ConcatMany(int.MinValue.To_4_bytes_array_BigEndian(), int.MinValue.To_4_bytes_array_BigEndian()), true                                      
                                      ))
                    {
                        //Getting the last version of the document
                        //We give ability to restore even deleted document
                        //if(row.Value == 1)  //Document was deleted (when its last version is marked as deleted)
                        //    break;
                        if (versionNumber == -1)
                            versionNumber = row.Key.Substring(4, 4).To_Int32_BigEndian();

                        if (row.Key.Substring(8, 4).To_Int32_BigEndian() == sequentialId)
                        {
                            DocumentVersionToRestore = row.Key;
                            break;
                        }
                    }

                    if (DocumentVersionToRestore == null || versionNumber < 1)  //Document was not found by supplied parameters
                        return false;

                    //We got the latest version of this document
                    //Reading it from dt by sequentialId of the 
                    var docRow = dt.Select<int, byte[]>(DocumentVersionToRestore.Substring(8, 4).To_Int32_BigEndian());
                    if (docRow == null)
                        return false; //Row is not found, but it should not normally happen

                    //Creating a clone of the latest document meatdata having new sequential ID
                    var MaxDocId = tran.Select<int, int>(DocTableName, 4).Value;
                    MaxDocId++;
                    tran.Insert<int, int>(DocTableName, 4,MaxDocId);
                    dt.Insert<int, byte[]>(MaxDocId, docRow.Value);

                    //Insert into P
                    tran.Insert<byte[], byte>(DocumentsStorageTablesPrefix + "p", docSpaceId.To_8_bytes_array_BigEndian().Concat(MaxDocId.To_4_bytes_array_BigEndian()), 0);

                    //Inserting into version table
                    vt.Insert<byte[], byte[]>(internalId.To_4_bytes_array_BigEndian().ConcatMany
                                        (
                                            (versionNumber + 1).To_4_bytes_array_BigEndian(),
                                            MaxDocId.To_4_bytes_array_BigEndian()
                                        ), new byte[] { 0 });


                    tran.Commit();
                }

                //Starting document indexing
                StartDocumentsIndexing();

                return true;
            }
            catch (Exception ex)
            {                
                throw ex;
            }
            
        }

                
        /// <summary>
        /// 
        /// </summary>
        /// <param name="req"></param>
        public SearchResponse SearchDocumentSpace(SearchRequest req)
        {
            SearchResponse resp = new SearchResponse();
            try
            {
                if (req == null || String.IsNullOrEmpty(req.DocumentSpace) || String.IsNullOrEmpty(req.SearchWords))
                    return resp;

                resp.DocumentSpace = req.DocumentSpace;

                Dictionary<int, Document> dmnts = new Dictionary<int, Document>();

                Action repack = () =>
                {
                    //Repacking dmnts into resp
                    if (req.IncludeDocuments)
                    {
                        foreach (var el in dmnts)
                        {
                            resp.Documents.Add(el.Value);
                        }
                    }
                    else
                    {
                        foreach (var el in dmnts)
                        {
                            resp.DocumentsInternalIds.Add(el.Key);
                        }
                    }
                };

                System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                sw.Start();

                using (var tran = DBreezeEngine.GetTransaction())
                {
                    var mt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
                    var docSpaceId = mt.Select<string, long>(req.DocumentSpace).Value;

                    if (docSpaceId == 0)
                        return resp;    //Not found document space


                    var Words = this.PrepareSearchKeyWords(req.SearchWords);

                    string docTable = DocumentsStorageTablesPrefix + "d" + docSpaceId.ToString();
                    var vt = tran.SelectTable<int>(docTable, 3, 0); //Version table Key
                    var dt = tran.SelectTable<int>(docTable, 1, 0); //Document table Key
                    dt.ValuesLazyLoadingIsOn = !req.IncludeDocuments;

                    DBreeze.DataTypes.Row<int, byte[]> docRow = null;
                    Document doc = null;
                    //byte[] btDoc = null;
                    int qOutput = 0;


                    //-----------------------------------------------------------------   ONE/MULTIPLE WORDS SEARCH then one word is supplied, using AND/OR LOGIC

                    #region "Multiple Words"

                    int j = -1;
                    List<byte[]> foundArrays = new List<byte[]>();
                    List<byte[]> oneWordFoundArrays = new List<byte[]>();
                    //WAH2 wh = null;
                    var tbOneWordWAH = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "s" + docSpaceId.ToString(), 2, 0);
                    tbOneWordWAH.ValuesLazyLoadingIsOn = false;

                    resp.UniqueWordsInDataSpace = (int)tbOneWordWAH.Count();

                    bool anyWordFound = false;
                    int totalFoundWords = 0;

                    Dictionary<string, WordInDoc> words = new Dictionary<string, WordInDoc>();
                    int foundOrigin = 1;

                    Dictionary<string, WordInDoc> perWord = new Dictionary<string, WordInDoc>();
                    Dictionary<string, WordInDoc> firstHighOccuranceWord = new Dictionary<string, WordInDoc>();

                    //Currently we ignore these words and do nothing with them
                    List<string> highOccuranceWordParts = new List<string>();

                    foreach (var word in Words.Take(10)) //Maximum 10 words for search
                    {
                        anyWordFound = false;
                        totalFoundWords = 0;
                        perWord = new Dictionary<string, WordInDoc>();

                        foreach (var row1 in tbOneWordWAH.SelectForwardStartsWith<string, byte[]>(word))
                        {
                            anyWordFound = true;
                            totalFoundWords++;
                            
                            if (Words.Count() == 1 && totalFoundWords > req.Quantity)
                            {
                                //In case if only one search word, then we don't need to make any comparation
                                break;
                            }
                            else if (totalFoundWords >= req.MaximalExcludingOccuranceOfTheSearchPattern)  //Found lots of words with such mask inside
                            {
                                //Too much found docs have this word-part inside, better to enhance search
                                if (firstHighOccuranceWord.Count() == 0)
                                {
                                    //Only first HighOccurance word part come to the list. It can be used later in case if all search words are of HighOccurance (then we will visualize only this one)
                                    firstHighOccuranceWord = perWord.ToDictionary(r => r.Key, r => r.Value);
                                }
                                //Clearing repack element
                                perWord.Clear();
                                //Adding word into List of High-Occurance word-part
                                highOccuranceWordParts.Add(word);
                                break;
                            }

                            perWord.Add(row1.Key, new WordInDoc()
                            {
                                 BlockId = row1.Value.Substring(0,4).To_Int32_BigEndian(),
                                 NumberInBlock = row1.Value.Substring(4, 4).To_Int32_BigEndian(),
                                 foundOrigin = foundOrigin
                            });
                        }

                        //Repacking occurances
                        foreach (var pw in perWord)
                            words.Add(pw.Key, pw.Value);

                        foundOrigin++;

                        if (
                            req.SearchLogicType == SearchRequest.eSearchLogicType.AND
                            &&
                            !anyWordFound
                            )
                        {
                            //Non of words found corresponding to AND logic
                            sw.Stop();
                            resp.SearchDurationMs = sw.ElapsedMilliseconds;
                            return resp;
                        }
                    }


                    if (words.Count() == 0)
                    {
                        //In case of multiple search words and each of them of HighOccurance.
                        //We will form result only from the first HighOccurance list

                        //Repacking occurances
                        foreach (var pw in firstHighOccuranceWord.Take(req.Quantity))
                            words.Add(pw.Key, pw.Value);

                        //In this case highOccuranceWordParts must be cleared, because the returning result is very approximate
                        highOccuranceWordParts.Clear();
                    }


                    //Here we must start get data from blocks
                    //Nested table with blocks
                    var tbBlocks = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "s" + docSpaceId.ToString(), 10, 0);
                    tbBlocks.ValuesLazyLoadingIsOn = false;

                    Dictionary<int,byte[]> block=null;
                    byte[] btBlock=null;
                    int currentBlockId = 0;

                    //DBreeze.Diagnostic.SpeedStatistic.StartCounter("LoadBlocks");
                        
                    foreach (var wrd in words.OrderBy(r=>r.Value.BlockId))
                    {
                        if (currentBlockId != wrd.Value.BlockId)
                        {
                            currentBlockId = wrd.Value.BlockId;

                                //DBreeze.Diagnostic.SpeedStatistic.StartCounter("SelectBlocks");
                            btBlock = tbBlocks.Select<int, byte[]>(wrd.Value.BlockId).Value;
                                //DBreeze.Diagnostic.SpeedStatistic.StopCounter("SelectBlocks");                            
                            btBlock = btBlock.Substring(4, btBlock.Substring(0, 4).To_Int32_BigEndian());
                                //DBreeze.Diagnostic.SpeedStatistic.StartCounter("DecomDeserBlocks");
                            btBlock = btBlock.DecompressGZip();                            
                            block = btBlock.DeserializeProtobuf<Dictionary<int, byte[]>>();
                                //DBreeze.Diagnostic.SpeedStatistic.StopCounter("DecomDeserBlocks");
                        }

                        wrd.Value.wah = new WAH2(block[wrd.Value.NumberInBlock]);
                    }
                    //DBreeze.Diagnostic.SpeedStatistic.PrintOut("LoadBlocks", true);
                    //DBreeze.Diagnostic.SpeedStatistic.PrintOut("SelectBlocks", true);
                    //DBreeze.Diagnostic.SpeedStatistic.PrintOut("DecomDeserBlocks", true);

                    foundOrigin = 0;

                    foreach (var wrd in words.OrderBy(r => r.Value.foundOrigin))
                    {
                        //Console.WriteLine(wrd.Value.foundOrigin);

                        if (foundOrigin != wrd.Value.foundOrigin)
                        {
                            if (oneWordFoundArrays.Count() > 0)
                            {
                                j++;
                                foundArrays.Add(WAH2.MergeAllUncompressedIntoOne(oneWordFoundArrays));
                                oneWordFoundArrays = new List<byte[]>();
                            }

                            foundOrigin = wrd.Value.foundOrigin;
                        }
                        else
                        {

                        }
                        
                        oneWordFoundArrays.Add(wrd.Value.wah.GetUncompressedByteArray());
                    }

                    //The last 
                    if (oneWordFoundArrays.Count() > 0)
                    {
                        j++;
                        foundArrays.Add(WAH2.MergeAllUncompressedIntoOne(oneWordFoundArrays));
                        oneWordFoundArrays = new List<byte[]>();
                    }
                      
              
                    //////////  final results

                    if (j >= 0)
                    {
                        var q = WAH2.TextSearch_OR_logic(foundArrays, req.Quantity);

                        if (req.SearchLogicType == SearchRequest.eSearchLogicType.AND)
                            q = WAH2.TextSearch_AND_logic(foundArrays).Take(req.Quantity);

                      
                        foreach (var el in q)
                        {
                            //Getting document
                            docRow = dt.Select<int, byte[]>((int)el);
                            if (docRow.Exists)
                            {
                                if (!dmnts.ContainsKey((int)el))
                                {
                                    if (highOccuranceWordParts.Count() > 0)
                                    {
                                        //We got some noisy word-parts of high occurance together with strongly found words.
                                        //We must be sure that these word parts are also inside of returned docs
                                        doc = this.RetrieveDocument(req.IncludeDocumentsContent, true, dt, docRow);
                                        if (doc != null)
                                        {
                                            //Checking doc.Searchables must have all word parts from the occurance in case of AND
                                            if (req.SearchLogicType == SearchRequest.eSearchLogicType.AND)
                                            {
                                                if (String.IsNullOrEmpty(doc.Searchables))
                                                    continue;
                                                if (!highOccuranceWordParts.All(doc.Searchables.ToLower().Contains))
                                                    continue;                                              
                                            }
                                            
                                            if (req.IncludeDocuments)
                                            {
                                                if (!req.IncludeDocumentsSearchanbles)
                                                    doc.Searchables = String.Empty;

                                                dmnts.Add((int)el, doc);
                                            }
                                            else
                                            {
                                                dmnts.Add((int)el, null);
                                            }

                                        }
                                        else
                                            continue;
                                    }
                                    else
                                    {
                                        if (req.IncludeDocuments)
                                        {

                                            doc = this.RetrieveDocument(req.IncludeDocumentsContent, req.IncludeDocumentsSearchanbles, dt, docRow);
                                            if (doc == null) //If doc is deleted, while search was in progress and we received its id in the list
                                                continue;

                                            dmnts.Add((int)el, doc);
                                        }
                                        else
                                        {
                                            dmnts.Add((int)el, null);                                           
                                        }

                                    }
                                    
                                    qOutput++;
                                }
                            }

                            if (qOutput > req.Quantity)
                                break;

                        }

                    }
                    #endregion


                }//eo using


                //Repacking dmnts into resp
                repack();
                sw.Stop();

                resp.SearchDurationMs = sw.ElapsedMilliseconds;
            }
            catch (Exception ex)
            {
                throw ThrowException("SearchDocumentSpace", ex.ToString());
            }

            return resp;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="req"></param>
        //public SearchResponse SearchDocumentSpace(SearchRequest req)
        //{
        //    SearchResponse resp = new SearchResponse();
        //    try
        //    {
        //        if (req == null || String.IsNullOrEmpty(req.DocumentSpace) || String.IsNullOrEmpty(req.SearchWords))
        //            return resp;

        //        resp.DocumentSpace = req.DocumentSpace;

        //        Dictionary<int, Document> dmnts = new Dictionary<int, Document>();

        //        Action repack = () =>
        //        {
        //            //Repacking dmnts into resp
        //            if (req.IncludeDocuments)
        //            {
        //                foreach (var el in dmnts)
        //                {
        //                    resp.Documents.Add(el.Value);
        //                }
        //            }
        //            else
        //            {
        //                foreach (var el in dmnts)
        //                {
        //                    resp.DocumentsInternalIds.Add(el.Key);
        //                }
        //            }
        //        };

        //        System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
        //        sw.Start();

        //        using (var tran = DBreezeEngine.GetTransaction())
        //        {
        //            var mt = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "m", 1, 0);
        //            var docSpaceId = mt.Select<string, long>(req.DocumentSpace).Value;

        //            if (docSpaceId == 0)
        //                return resp;    //Not found document space


        //            var Words = this.PrepareSearchKeyWords(req.SearchWords);

        //            string docTable = DocumentsStorageTablesPrefix + "d" + docSpaceId.ToString();
        //            var vt = tran.SelectTable<int>(docTable, 3, 0); //Version table Key
        //            var dt = tran.SelectTable<int>(docTable, 1, 0); //Document table Key
        //            dt.ValuesLazyLoadingIsOn = !req.IncludeDocuments;

        //            DBreeze.DataTypes.Row<int, byte[]> docRow = null;
        //            Document doc = null;
        //            //byte[] btDoc = null;
        //            int qOutput = 0;


        //            //-----------------------------------------------------------------   ONE/MULTIPLE WORDS SEARCH then one word is supplied, using AND/OR LOGIC

        //            #region "Multiple Words"

        //            int j = -1;
        //            List<byte[]> foundArrays = new List<byte[]>();
        //            List<byte[]> oneWordFoundArrays = new List<byte[]>();
        //            //WAH2 wh = null;
        //            var tbOneWordWAH = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "s" + docSpaceId.ToString(), 2, 0);
        //            tbOneWordWAH.ValuesLazyLoadingIsOn = false;

        //            resp.UniqueWordsInDataSpace = (int)tbOneWordWAH.Count();

        //            bool anyWordFound = false;
        //            int totalFoundWords = 0;

        //            Dictionary<string, WordInDoc> words = new Dictionary<string, WordInDoc>();
        //            int foundOrigin = 1;

        //            Dictionary<string, WordInDoc> perWord = new Dictionary<string, WordInDoc>();

        //            foreach (var word in Words)
        //            {
        //                anyWordFound = false;
        //                totalFoundWords = 0;
        //                perWord = new Dictionary<string, WordInDoc>();

        //                foreach (var row1 in tbOneWordWAH.SelectForwardStartsWith<string, byte[]>(word))
        //                {
        //                    anyWordFound = true;
        //                    totalFoundWords++;

        //                    if (Words.Count() == 1 && totalFoundWords >= req.Quantity)
        //                    {
        //                        //In case if only one search word, then we don't need to make any comparat
        //                        break;
        //                    }
        //                    else if (totalFoundWords >= req.MaximalExcludingOccuranceOfTheSearchPattern)  //Found lots of words with such mask inside
        //                    {
        //                        //Too much found docs have this word part inside, better to enhance search
        //                        break;
        //                    }

        //                    perWord.Add(row1.Key, new WordInDoc()
        //                    {
        //                        BlockId = row1.Value.Substring(0, 4).To_Int32_BigEndian(),
        //                        NumberInBlock = row1.Value.Substring(4, 4).To_Int32_BigEndian(),
        //                        foundOrigin = foundOrigin
        //                    });
        //                }

        //                //Repacking occurances
        //                if (totalFoundWords < req.MaximalExcludingOccuranceOfTheSearchPattern)
        //                {
        //                    foreach (var pw in perWord)
        //                        words.Add(pw.Key, pw.Value);
        //                }

        //                foundOrigin++;

        //                if (
        //                    req.SearchLogicType == SearchRequest.eSearchLogicType.AND
        //                    &&
        //                    !anyWordFound
        //                    )
        //                {
        //                    //Non of words found corresponding to AND logic
        //                    sw.Stop();
        //                    resp.SearchDurationMs = sw.ElapsedMilliseconds;
        //                    return resp;
        //                }
        //            }


        //            //Here we must start get data from blocks
        //            //Nested table with blocks
        //            var tbBlocks = tran.SelectTable<int>(DocumentsStorageTablesPrefix + "s" + docSpaceId.ToString(), 10, 0);
        //            tbBlocks.ValuesLazyLoadingIsOn = false;

        //            Dictionary<int, byte[]> block = null;
        //            byte[] btBlock = null;
        //            int currentBlockId = 0;

        //            //DBreeze.Diagnostic.SpeedStatistic.StartCounter("LoadBlocks");

        //            foreach (var wrd in words.OrderBy(r => r.Value.BlockId))
        //            {
        //                if (currentBlockId != wrd.Value.BlockId)
        //                {
        //                    currentBlockId = wrd.Value.BlockId;

        //                    //DBreeze.Diagnostic.SpeedStatistic.StartCounter("SelectBlocks");
        //                    btBlock = tbBlocks.Select<int, byte[]>(wrd.Value.BlockId).Value;
        //                    //DBreeze.Diagnostic.SpeedStatistic.StopCounter("SelectBlocks");                            
        //                    btBlock = btBlock.Substring(4, btBlock.Substring(0, 4).To_Int32_BigEndian());
        //                    //DBreeze.Diagnostic.SpeedStatistic.StartCounter("DecomDeserBlocks");
        //                    btBlock = btBlock.DecompressGZip();
        //                    block = btBlock.DeserializeProtobuf<Dictionary<int, byte[]>>();
        //                    //DBreeze.Diagnostic.SpeedStatistic.StopCounter("DecomDeserBlocks");
        //                }

        //                wrd.Value.wah = new WAH2(block[wrd.Value.NumberInBlock]);
        //            }
        //            //DBreeze.Diagnostic.SpeedStatistic.PrintOut("LoadBlocks", true);
        //            //DBreeze.Diagnostic.SpeedStatistic.PrintOut("SelectBlocks", true);
        //            //DBreeze.Diagnostic.SpeedStatistic.PrintOut("DecomDeserBlocks", true);

        //            foundOrigin = 0;

        //            foreach (var wrd in words.OrderBy(r => r.Value.foundOrigin))
        //            {
        //                //Console.WriteLine(wrd.Value.foundOrigin);

        //                if (foundOrigin != wrd.Value.foundOrigin)
        //                {
        //                    if (oneWordFoundArrays.Count() > 0)
        //                    {
        //                        j++;
        //                        foundArrays.Add(WAH2.MergeAllUncompressedIntoOne(oneWordFoundArrays));
        //                        oneWordFoundArrays = new List<byte[]>();
        //                    }

        //                    foundOrigin = wrd.Value.foundOrigin;
        //                }
        //                else
        //                {

        //                }

        //                oneWordFoundArrays.Add(wrd.Value.wah.GetUncompressedByteArray());
        //            }

        //            //The last 
        //            if (oneWordFoundArrays.Count() > 0)
        //            {
        //                j++;
        //                foundArrays.Add(WAH2.MergeAllUncompressedIntoOne(oneWordFoundArrays));
        //                oneWordFoundArrays = new List<byte[]>();
        //            }


        //            if (j >= 0)
        //            {
        //                var q = WAH2.TextSearch_OR_logic(foundArrays, req.Quantity);

        //                if (req.SearchLogicType == SearchRequest.eSearchLogicType.AND)
        //                    q = WAH2.TextSearch_AND_logic(foundArrays).Take(req.Quantity);

        //                foreach (var el in q)
        //                {
        //                    //Getting document
        //                    docRow = dt.Select<int, byte[]>((int)el);
        //                    if (docRow.Exists)
        //                    {
        //                        if (!dmnts.ContainsKey((int)el))
        //                        {
        //                            if (req.IncludeDocuments)
        //                            {

        //                                doc = this.RetrieveDocument(req.IncludeDocumentsContent, req.IncludeDocumentsSearchanbles, dt, docRow);
        //                                if (doc != null) //If doc is deleted, while search was in progress and we received its id in the list
        //                                    dmnts.Add((int)el, doc);
        //                            }
        //                            else
        //                            {
        //                                dmnts.Add((int)el, null);
        //                            }
        //                            qOutput++;
        //                        }
        //                    }

        //                    if (qOutput > req.Quantity)
        //                        break;

        //                }

        //            }
        //            #endregion


        //        }//eo using


        //        //Repacking dmnts into resp
        //        repack();
        //        sw.Stop();

        //        resp.SearchDurationMs = sw.ElapsedMilliseconds;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ThrowException("SearchDocumentSpace", ex.ToString());
        //    }

        //    return resp;
        //}

        /// <summary>
        /// Only distinct only 
        /// </summary>
        /// <param name="searchKeywords"></param>
        private HashSet<string> PrepareSearchKeyWords(string searchKeywords)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                HashSet<string> words = new HashSet<string>();
                string word = String.Empty;

                Action processWord = () =>
                {
                    if (sb.Length > 0)
                    {
                        word = sb.ToString().ToLower();
                        if(!words.Contains(word))
                            words.Add(word);
                    }

                    if (sb.Length > 0)
                        sb.Remove(0, sb.Length);
                    //sb.Clear();
                };

                foreach (var c in searchKeywords)
                {
                    if (c == '-' || c == '@')   //Complex names or email address inside
                        continue;

                    if (Char.IsLetterOrDigit(c) || Char.IsSymbol(c))
                    {
                        sb.Append(c);
                    }
                    else
                    {
                        processWord();
                    }
                }

                //Handling last word
                processWord();

                return words;
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

    }//eoc
}
