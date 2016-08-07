using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Sql
{
   internal class ContinuationToken : IContinuationToken
   {
      private long _skip;
      private int _segmentSize;
      private bool _hasMore;

      public ContinuationToken( bool hasMore, long skip, int segmentSize )
      {
         _hasMore = hasMore;
         _skip = skip;
         _segmentSize = segmentSize;
      }
      
      public long Skip
      {
         get
         {
            return _skip;
         }
      }

      public bool HasMore
      {
         get
         {
            return _hasMore;
         }
      }

      public void SkippedWasDeleted()
      {
         _skip -= _segmentSize;
      }
   }
}
