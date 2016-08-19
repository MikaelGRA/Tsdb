using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.InfluxDB
{
   internal class ContinuationToken : IContinuationToken
   {
      private bool _hasMore;
      private DateTime? _to;

      public ContinuationToken( bool hasMore, DateTime? to )
      {
         _hasMore = hasMore;
         _to = to;
      }

      public DateTime? At
      {
         get
         {
            return _to;
         }
      }

      public bool HasMore
      {
         get
         {
            return _hasMore;
         }
      }
   }
}
