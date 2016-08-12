using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IDynamicStorage<TKey, TEntry> : IStorage<TKey, TEntry> where TEntry : IEntry<TKey>
   {
      //Task<SegmentedReadResult<TKey, TEntry>> Read( TKey id, DateTime to, int segmentSize, object continuationToken );

      //Task<SegmentedReadResult<TKey, TEntry>> Read( TKey id, int segmentSize, object continuationToken );

      Task<SegmentedReadResult<TKey, TEntry>> Read( TKey id, DateTime? from, DateTime? to, int segmentSize, object continuationToken );

      Task<MultiReadResult<TKey, TEntry>> Read( IEnumerable<TKey> ids, DateTime to, Sort sort = Sort.Descending );

      Task Delete( IEnumerable<TKey> ids, DateTime to );
   }
}
