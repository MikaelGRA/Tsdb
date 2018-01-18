using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IStorage<TKey, TEntry> where TEntry : IEntry
   {
      Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> series );

      Task DeleteAsync( IEnumerable<TKey> ids );

      Task DeleteAsync( IEnumerable<TKey> ids, DateTime to );

      Task DeleteAsync( IEnumerable<TKey> ids, DateTime from, DateTime to );

      Task<MultiReadResult<TKey, TEntry>> ReadLatestAsync( IEnumerable<TKey> ids, int count );

      Task<MultiReadResult<TKey, TEntry>> ReadLatestSinceAsync( IEnumerable<TKey> ids, DateTime to, int count, Sort sort = Sort.Descending );

      Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, Sort sort = Sort.Descending );

      Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime to, Sort sort = Sort.Descending );

      Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending );
   }
}
