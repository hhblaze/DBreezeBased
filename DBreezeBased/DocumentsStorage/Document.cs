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
    /// DBreezeBased.DocumentsStorage.Document
    /// </summary>
    [ProtoBuf.ProtoContract]
    public class Document
    {

        /// <summary>
        /// Logical grouping of documents (correctly would be "document groups") to distinguish search space.
        /// Must field while inserting and searching of documents.
        /// </summary>
        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public string DocumentSpace { get; set; }
        /// <summary>
        /// External name to visualize document (must NOT be unique)
        /// </summary>
        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public string DocumentName { get; set; }
        //public string Name { get; set; }
        /// <summary>
        /// Content of the document (can converted into byte[] anything, which will be returned back by request)
        /// </summary>
        [ProtoBuf.ProtoMember(3, IsRequired = true)]
        public byte[] Content { get; set; }
        /// <summary>
        /// External document group ID.  Group is a set of all versions of one document.
        /// Such id can represet document for outer system number scope.        
        /// </summary>
        [ProtoBuf.ProtoMember(4, IsRequired = true)]
        public string ExternalId { get; set; }
        /// <summary>
        /// Document group id. Group is a set of all versions of one document.
        /// </summary>
        [ProtoBuf.ProtoMember(5, IsRequired = true)]
        public int InternalId { get; set; }
        /// <summary>
        /// Text (separated words/digits/symbols). 
        /// They will be used for the future search the docuemnt among document space. 
        /// </summary>
        [ProtoBuf.ProtoMember(6, IsRequired = true)]
        public string Searchables { get; set; }     
        /// <summary>
        /// After inserting document receives also DocumentSpaceId
        /// </summary>
        [ProtoBuf.ProtoMember(7, IsRequired = true)]
        public long DocumentSpaceId { get; set; }
        /// <summary>
        /// Monotonically grown internal id. Every inserted or updated docuemnt receives new DocumentSequentialId.
        /// </summary>
        [ProtoBuf.ProtoMember(8, IsRequired = true)]
        public int DocumentSequentialId { get; set; }
        /// <summary>
        /// For internal usage. First 16 bytes link to searchables
        /// </summary>
        [ProtoBuf.ProtoMember(9, IsRequired = true)]
        public byte[] InternalStructure { get; set; }
        /// <summary>
        /// Can be supplied extra document description
        /// </summary>
        [ProtoBuf.ProtoMember(10, IsRequired = true)]
        public string Description { get; set; }

    }
}
