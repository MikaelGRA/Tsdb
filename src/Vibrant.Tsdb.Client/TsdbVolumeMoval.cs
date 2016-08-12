using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   public class TsdbVolumeMoval<TKey>
   {
      public TsdbVolumeMoval( TKey id, DateTime timestamp, DateTime to, TimeSpan storageExpiration )
      {
         Id = id;
         Timestamp = timestamp;
         To = to;
         StorageExpiration = storageExpiration;
      }

      public TKey Id { get; private set; }

      public DateTime Timestamp { get; private set; }

      public DateTime To { get; private set; }

      public TimeSpan StorageExpiration { get; private set; }
   }
}
