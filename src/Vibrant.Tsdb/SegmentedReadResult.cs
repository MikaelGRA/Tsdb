using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class SegmentedReadResult<TEntry> : ReadResult<TEntry>
     where TEntry : IEntry
   {
      public SegmentedReadResult( string id, Sort sort, object continuationToken, List<TEntry> entries )
         : base( id, sort, entries )
      {
         ContinuationToken = continuationToken;
      }

      public SegmentedReadResult( string id, Sort sort, object continuationToken )
         : base( id, sort )
      {
         ContinuationToken = continuationToken;
      }

      public object ContinuationToken { get; private set; }

      public new ReadResult<TOutputEntry> As<TOutputEntry>()
         where TOutputEntry : IEntry
      {
         return new SegmentedReadResult<TOutputEntry>( Id, Sort, Entries.Cast<TOutputEntry>().ToList() );
      }
   }
}
