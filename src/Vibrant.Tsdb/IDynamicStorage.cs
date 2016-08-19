using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IDynamicStorage<TKey, TEntry> : IStorage<TKey, TEntry>
      where TEntry : IEntry
   {
      Task<SegmentedReadResult<TKey, TEntry>> ReadSegmentedAsync( TKey id, DateTime? from, DateTime? to, int segmentSize, IContinuationToken continuationToken );

      Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime to, Sort sort = Sort.Descending );

      Task DeleteAsync( IEnumerable<TKey> ids, DateTime to );
   }

   public interface IReversableDynamicStorage<TKey, TEntry>
      where TEntry : IEntry
   {
      Task<SegmentedReadResult<TKey, TEntry>> ReadReverseSegmentedAsync( TKey id, DateTime? from, DateTime? to, int segmentSize, IContinuationToken continuationToken );
   }
}
