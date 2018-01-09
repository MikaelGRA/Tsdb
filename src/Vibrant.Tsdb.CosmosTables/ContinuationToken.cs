using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.CosmosTables
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

      public DateTime? To
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
