using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   internal class AtsSerializationResult
   {
      public AtsSerializationResult( DateTime from, byte[] data )
      {
         From = from;
         Data = data;
      }

      public DateTime From { get; private set; }

      public byte[] Data { get; private set; }
   }
}
