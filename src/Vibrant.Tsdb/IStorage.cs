using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IStorage<TEntry> where TEntry : IEntry
   {
      Task Write( IEnumerable<TEntry> items );

      Task Delete( IEnumerable<string> ids, DateTime from, DateTime to );

      Task Delete( IEnumerable<string> ids );

      Task<MultiReadResult<TEntry>> ReadLatest( IEnumerable<string> ids );

      Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, Sort sort = Sort.Descending );

      Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending );

      //Task<SegmentedReadResult<TEntry>> Read( string id, DateTime from, DateTime to, int segmentSize, object continuationToken );
   }
}
