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
    public class SearchRequest
    {
        public enum eSearchLogicType
        {
            /// <summary>
            /// Strong. Only complete occurance is accepted
            /// </summary>
            AND,
            /// <summary>
            /// Week logic, 1 and more occurances are accepted, sorted by relevancy of occurances
            /// </summary>
            OR
        }

        public SearchRequest()
        {
            DocumentSpace = String.Empty;
            SearchWords = String.Empty;
            SearchLogicType = eSearchLogicType.OR;
            Quantity = 100;
            IncludeDocuments = false;
            IncludeDocumentsSearchanbles = false;
            IncludeDocumentsContent = true;
            MaximalExcludingOccuranceOfTheSearchPattern = 1000;
        }


        /// <summary>
        /// Doucument space which must be searched
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public string DocumentSpace { get; set; }

        /// <summary>
        /// Words separated by space or whatever to search
        /// </summary>
        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public string SearchWords { get; set; }

        /// <summary>
        /// Results quantity. Lower value - lower RAM and speed economy.
        /// </summary>
        [ProtoBuf.ProtoMember(3, IsRequired = true)]
        public int Quantity { get; set; }

        /// <summary>
        /// AND/OR. Default OR
        /// </summary>
        [ProtoBuf.ProtoMember(4, IsRequired = true)]
        public eSearchLogicType SearchLogicType { get; set; }

        /// <summary>
        /// Include complete documents or just internal documents IDs. Default is true
        /// </summary>
        [ProtoBuf.ProtoMember(5, IsRequired = true)]
        public bool IncludeDocuments { get; set; }

        /// <summary>
        /// Include document content (default true). Only matters together with IncludeDocuments = true;
        /// </summary>
        [ProtoBuf.ProtoMember(6, IsRequired = true)]
        public bool IncludeDocumentsContent { get; set; }

        /// <summary>
        /// Include document searchables (default false). Only matters together with IncludeDocuments = true;
        /// </summary>
        [ProtoBuf.ProtoMember(7, IsRequired = true)]
        public bool IncludeDocumentsSearchanbles { get; set; }

        /// <summary>
        /// Default value is 10000. It means if such word's Starts With found more than "MaximalExcludingOccuranceOfTheSearchPattern" times, 
        /// it will be excluded from the search.
        /// For example, after uploading 122 russian books and having 700000 unique words, we try to search combination of "ал".
        /// We have found it 3240 times:
        /// ал
        /// ала
        /// алабан
        /// алаберная
        /// алаберный
        /// алаболки
        /// алаболь
        /// ...
        /// etc.
        /// !This is not the quantity of documents where such pattern exists, but StartsWith result of all unique words in the document space!
        /// </summary>
        [ProtoBuf.ProtoMember(8, IsRequired = true)]
        public uint MaximalExcludingOccuranceOfTheSearchPattern { get; set; }
    }
   
}
