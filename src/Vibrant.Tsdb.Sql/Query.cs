using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Sql
{
   internal class Query
   {
      public string Sql { get; set; }

      public object Args { get; set; }
   }
}
