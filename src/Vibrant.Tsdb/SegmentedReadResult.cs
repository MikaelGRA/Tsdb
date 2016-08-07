using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class SegmentedReadResult<TEntry> : ReadResult<TEntry>
     where TEntry : IEntry
   {
      private Func<Task> _delete;

      public SegmentedReadResult( string id, Sort sort, IContinuationToken continuationToken, List<TEntry> entries, Func<Task> delete )
         : base( id, sort, entries )
      {
         ContinuationToken = continuationToken;
         _delete = delete;
      }

      public IContinuationToken ContinuationToken { get; private set; }

      public Task DeleteAsync()
      {
         return _delete();
      }
   }
}
