using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Sql
{
   public class SqlEntry
   {
      public string Id { get; set; }

      public DateTime Timestamp { get; set; }

      public byte[] Data { get; set; }
   }
}
