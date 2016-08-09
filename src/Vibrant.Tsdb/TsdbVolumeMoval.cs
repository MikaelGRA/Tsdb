using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbVolumeMoval<TKey>
   {
      public TsdbVolumeMoval( TKey id, DateTime timestamp, DateTime to )
      {
         Id = id;
         Timestamp = timestamp;
         To = to;
      }

      public TKey Id { get; private set; }

      public DateTime Timestamp { get; private set; }

      public DateTime To { get; private set; }
   }
}
