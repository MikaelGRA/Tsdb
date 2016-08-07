using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IDynamicStorage<TEntry> : IStorage<TEntry> where TEntry : IEntry
   {
      Task<SegmentedReadResult<TEntry>> Read( string id, DateTime to, int segmentSize, object continuationToken );

      Task<SegmentedReadResult<TEntry>> Read( string id, int segmentSize, object continuationToken );

      Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime to, Sort sort = Sort.Descending );

      Task Delete( IEnumerable<string> ids, DateTime to );
   }
}
