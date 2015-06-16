using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VisualTester
{
    [ProtoBuf.ProtoContract]
    public class Customer
    {
        public Customer()
        {
            Id = 0;
            Name = String.Empty;
            Surname = String.Empty;
            Phone = String.Empty;
        }

        [ProtoBuf.ProtoMember(1, IsRequired = true)]
        public long Id { get; set; }
                
        [ProtoBuf.ProtoMember(2, IsRequired = true)]
        public string Name { get; set; }

        [ProtoBuf.ProtoMember(3, IsRequired = true)]
        public string Surname { get; set; }

        [ProtoBuf.ProtoMember(4, IsRequired = true)]
        public string Phone { get; set; }
    }    

}
