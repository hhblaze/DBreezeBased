using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

using DBreeze;
//using DBreezeBased;

namespace VisualTester
{
    public partial class Form1 : Form
    {
        DBreezeEngine engine = null;
        DBreezeBased.DocumentsStorage.Storage DocuStorage = null;

        public Form1()
        {
            InitializeComponent();
        }

        /// <summary>
        /// DB folder
        /// </summary>
        string DbPath = @"D:\temp\DBreezeTest\DBR1\";

        void initDB()
        {
            if (engine != null)
                return;

            engine = new DBreezeEngine(new DBreezeConfiguration()
            {
                 DBreezeDataFolderName = DbPath
            });

            
            //Setting up DBreeze to work with protobuf
            DBreeze.Utils.CustomSerializator.ByteArraySerializator = DBreezeBased.Serialization.ProtobufSerializer.SerializeProtobuf;
            DBreeze.Utils.CustomSerializator.ByteArrayDeSerializator = DBreezeBased.Serialization.ProtobufSerializer.DeserializeProtobuf;

            DocuStorage = new DBreezeBased.DocumentsStorage.Storage(this.engine);
            DocuStorage.VerboseConsoleEnabled = true;   //Make it write in console document processing progress
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button1_Click(object sender, EventArgs e)
        {
            this.initDB();

            //this.Test1();
            //this.Test2();
            //this.Test3();
            //this.Test4();
            //this.Test5();

        }

        /// <summary>
        /// Search documents
        /// </summary>
        void Test3()
        {
            try
            {
               // long docSpaceId = DocuStorage.GetDocumentSpaceId("SearchableCustomers", false);

                using (var tran = engine.GetTransaction())
                {
                    DBreezeBased.DocumentsStorage.SearchResponse res = null;

                    res = this.DocuStorage.SearchDocumentSpace(new DBreezeBased.DocumentsStorage.SearchRequest()
                    {
                        IncludeDocuments = true,
                        SearchLogicType = DBreezeBased.DocumentsStorage.SearchRequest.eSearchLogicType.AND,
                        DocumentSpace = "SearchableCustomers",
                        Quantity = 200,
                        MaximalExcludingOccuranceOfTheSearchPattern = 100,  //Must stay lower for low RAM systems (Mobile Phones), and bigger for servers 
                        SearchWords = "ang",  //returns ExternalId 1 where SearchLogicType is AND
                        //SearchWords = "ang 51",   //returns ExternalId 1 where SearchLogicType is AND
                        //SearchWords = "ang 53",   //returns ExternalId 1,2 where SearchLogicType is OR
                        //SearchWords = "ang 53",   //returns nothing where SearchLogicType is AND
                        //SearchWords = "040",   //returns ExternalId 3 where SearchLogicType is AND
                        IncludeDocumentsContent = false,
                        IncludeDocumentsSearchanbles = false
                    });


                    foreach (var rs in res.Documents)
                    {
                        //Here we can use rs.ExternalId to get from Customer table
                    }
                }
              
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        
        /// <summary>
        /// Adding documents
        /// </summary>
        void Test1()
        {
            DBreezeBased.DocumentsStorage.Document doc = null;
            List<DBreezeBased.DocumentsStorage.Document> docs = new List<DBreezeBased.DocumentsStorage.Document>();

            doc = new DBreezeBased.DocumentsStorage.Document()
            {
                DocumentSpace = "space1",
                //Content = new byte[1000],
                DocumentName = "name 1",
                ExternalId = "e1",
                Searchables = "table song",
                Description = "descr 1"
            };

            docs.Add(doc);

            doc = new DBreezeBased.DocumentsStorage.Document()
            {
                DocumentSpace = "space1",
               // Content = new byte[1000],
                DocumentName = "name 2",
                ExternalId = "e2",
                Searchables = "table plash",
                Description = "descr 2"
            };

            docs.Add(doc);


            doc = new DBreezeBased.DocumentsStorage.Document()
            {
                DocumentSpace = "space1",
                // Content = new byte[1000],
                DocumentName = "name 3",
                ExternalId = "e3",
                Searchables = "New England is digging out this morning after receiving more than 30 inches of snow in some areas from a major northeast storm. A travel ban was lifted at midnight in Massachusetts, but authorities are urging drivers to stay off the roads if necessary as cleanup efforts continue.",
                Description = "descr 3"
            };

            docs.Add(doc);

            var retdocs = DocuStorage.AddDocuments(docs);

            DocuStorage.StartDocumentsIndexing();

        }

        void Test5()
        {
            DBreezeBased.DocumentsStorage.Document doc = null;
            List<DBreezeBased.DocumentsStorage.Document> docs = new List<DBreezeBased.DocumentsStorage.Document>();

            doc = new DBreezeBased.DocumentsStorage.Document()
            {
                DocumentSpace = "space1",
                //Content = new byte[1000],
                DocumentName = "name 3",
                ExternalId = "e3",
                Searchables = "my bonny is over the ocean",
                Description = "descr 3"
            };

            docs.Add(doc);

            var retdocs = DocuStorage.AddDocuments(docs);

            DocuStorage.StartDocumentsIndexing();
            
        }

        void Test4()
        {
           // DBreezeBased.DocumentsStorage.Storage.InTran_DocumentAppender docAppender = null;

           
            var resp = this.DocuStorage.SearchDocumentSpace(new DBreezeBased.DocumentsStorage.SearchRequest()
                    {
                        IncludeDocuments = true,
                        SearchLogicType = DBreezeBased.DocumentsStorage.SearchRequest.eSearchLogicType.AND,
                        DocumentSpace = "space1",
                        Quantity = 200,
                        MaximalExcludingOccuranceOfTheSearchPattern = 100,  //Must stay lower for low RAM systems (Mobile Phones), and bigger for servers 
                        //SearchWords = "digging out this morning after",  //returns ExternalId 1 where SearchLogicType is AND                     
                        //SearchWords = "northeast storm. a travel ban",
                        SearchWords = "bonny is",
                        IncludeDocumentsContent = false,
                        IncludeDocumentsSearchanbles = false
                    });


                    foreach (var rs in resp.Documents)
                    {
                        //Here we can use rs.ExternalId to get from Customer table
                        Console.WriteLine(rs.ExternalId);
                    }
            
        }
               
        void Test2()
        {
            long docSpaceId = DocuStorage.GetDocumentSpaceId("SearchableCustomers", true);

            List<Customer> customers = new List<Customer>();

            Customer customer = new Customer()
            {
                Id = 1,    //Obtaining and handling this ID is out of the scope, please refer to DBreeze documentation
                Name = "Liu",
                Surname = "Kang",
                Phone = "040 411 51 51"
            };

          
            customers.Add(customer);

            customer = new Customer()
            {
                Id = 2,    //Obtaining and handling this ID is out of the scope, please refer to DBreeze documentation
                Name = "Johny",
                Surname = "Cage",
                Phone = "040 411 51 53"
            };

            customers.Add(customer);

            customer = new Customer()
            {
                Id = 3,    //Obtaining and handling this ID is out of the scope, please refer to DBreeze documentation
                Name = "Kung",
                Surname = "Lao",
                Phone = "040 411 51 56"
            };

            customers.Add(customer);

            //We assume that we add all this customers for search under one Document Search Space
            AddCustomers(customers, docSpaceId);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="customers"></param>
        /// <param name="documentSpaceId"></param>
        void AddCustomers(List<Customer> customers,long documentSpaceId)
        {
            try
            {
                DBreezeBased.DocumentsStorage.Storage.InTran_DocumentAppender docAppender = null;
                DBreezeBased.DocumentsStorage.Document doc = null;
            
                using (var tran = engine.GetTransaction())
                {
                     List<string> tbls = new List<string>();

                    tbls.Add("c");  //Customer table

                    //Blocking on write tables concerning DBreezeBased.DocumentsStorage
                    tbls.Add(DocuStorage.DocumentsStorageTablesPrefix + "d" + documentSpaceId.ToString());      //blocking documentSpace
                    tbls.Add(DocuStorage.DocumentsStorageTablesPrefix + "p");   //processing table 

                    tran.SynchronizeTables(tbls);

                    //Initializing docAppender, supplying transaction and DocuStorage.DocumentsStorageTablesPrefix
                    docAppender = new DBreezeBased.DocumentsStorage.Storage.InTran_DocumentAppender(tran, DocuStorage.DocumentsStorageTablesPrefix);                    

                    foreach (var customer in customers)
                    {                       
                        doc = new DBreezeBased.DocumentsStorage.Document()
                        {
                            //DocumentSpace = "SearchableCustomers",
                            ExternalId = customer.Id.ToString(),
                            Searchables = GetSearchablesFromCustomer(customer),
                            DocumentSpaceId = documentSpaceId
                        };

                        docAppender.AppendDocument(doc);

                        tran.Insert<long, Customer>("c", customer.Id, customer);
                    }

                    tran.Commit();
                }

                DocuStorage.StartDocumentsIndexing();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="customers"></param>
        /// <param name="documentSpaceId"></param>
        void RemoveCustomers(List<Customer> customers, long documentSpaceId)
        {
            try
            {
                DBreezeBased.DocumentsStorage.Storage.InTran_DocumentAppender docAppender = null;
                DBreezeBased.DocumentsStorage.Document doc = null;

                using (var tran = engine.GetTransaction())
                {
                    List<string> tbls = new List<string>();

                    tbls.Add("c");  //Customer table

                    //Blocking on write tables concerning DBreezeBased.DocumentsStorage
                    tbls.Add(DocuStorage.DocumentsStorageTablesPrefix + "d" + documentSpaceId.ToString());      //blocking documentSpace
                    tbls.Add(DocuStorage.DocumentsStorageTablesPrefix + "p");   //processing table 

                    tran.SynchronizeTables(tbls);

                    //Initializing docAppender, supplying transaction and DocuStorage.DocumentsStorageTablesPrefix
                    docAppender = new DBreezeBased.DocumentsStorage.Storage.InTran_DocumentAppender(tran, DocuStorage.DocumentsStorageTablesPrefix);

                    foreach (var customer in customers)
                    {                        
                        doc = new DBreezeBased.DocumentsStorage.Document()
                        {                            
                            ExternalId = customer.Id.ToString(),                            
                            DocumentSpaceId = documentSpaceId
                        };

                        docAppender.RemoveDocument(doc);
                        tran.RemoveKey<long>("c", customer.Id);
                    }

                    tran.Commit();
                }

                DocuStorage.StartDocumentsIndexing();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        string GetSearchablesFromCustomer(Customer customer)
        {
            if (customer == null)
                return String.Empty;

            return ((customer.Name ?? "") + " " + (customer.Surname ?? "") + " " + (customer.Phone ?? "")).Trim();
        }




    }
}
