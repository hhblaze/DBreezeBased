/* 
  Copyright (C) 2014 dbreeze.tiesky.com / Alex Solovyov / Ivars Sudmalis.
  It's a free software for those, who thinks that it should be free.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DBreezeBased.DocumentsStorage
{
    /// <summary>
    /// 
    /// </summary>
    [ProtoBuf.ProtoContract]
    public class SearchResponse
    {
        public enum eResultCode
        {
            OK,            
            EXCEPTION           
        }

        public SearchResponse()
        {
            ResultCode = eResultCode.OK;
            Documents = new List<Document>();
            DocumentsInternalIds = new List<int>();
            SearchCriteriaIsOverloaded = false;
        }

        /// <summary>
        /// ResultCode
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public eResultCode ResultCode { get; set; }

        /// <summary>
        /// Either Documents or DocumentsInternalIds, depending upon SearchRequest
        /// </summary>
        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public List<Document> Documents { get; set; }

        /// <summary>
        /// Either Documents or DocumentsInternalIds, depending upon SearchRequest
        /// </summary>
        [ProtoBuf.ProtoMember(3, IsRequired = true)]
        public List<int> DocumentsInternalIds { get; set; }

        /// <summary>
        /// Doucument space which was searched
        /// </summary>
        [ProtoBuf.ProtoMember(4, IsRequired = true)]
        public string DocumentSpace { get; set; }

        /// <summary>
        /// SearchDurationMs
        /// </summary>
        [ProtoBuf.ProtoMember(5, IsRequired = true)]
        public long SearchDurationMs = 0;
        /// <summary>
        /// UniqueWordsInDataSpace
        /// </summary>
        [ProtoBuf.ProtoMember(6, IsRequired = true)]
        public int UniqueWordsInDataSpace = 0;

        /// <summary>
        /// SearchCriteriaIsOverloaded. When one of words in search request contains more then 1000 intersections it will become true.
        /// It can mean that better is to change search word criteria.
        /// Lobster 
        /// Lopata
        /// ...
        /// Loshad
        /// Lom 
        /// .. e.g. words starting from "Lo" is more then 10000
        /// ......and we search by "Lo".
        /// This "Lo" will be automatically excluded from search?
        /// </summary>
        [ProtoBuf.ProtoMember(7, IsRequired = true)]
        public bool SearchCriteriaIsOverloaded { get; set; }


        public string VisualizeSearch()
        {
            int res = Documents.Count();
            if(res == 0)
                res = DocumentsInternalIds.Count();

            return String.Format("{0}, Found {1} docs, took {2}ms. Total words in document space: {3}", ResultCode.ToString(), res, SearchDurationMs, UniqueWordsInDataSpace);
        }
    }
}
