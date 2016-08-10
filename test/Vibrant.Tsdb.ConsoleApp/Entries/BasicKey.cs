using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.ConsoleApp.Entries
{
   public struct BasicKey
   {
      public Guid Id { get; set; }

      public Sampling Sampling { get; set; }
   }
}
