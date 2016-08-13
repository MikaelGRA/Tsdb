using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IStorage<TKey, TEntry> where TEntry : IEntry<TKey>
   {
      Task WriteAsync( IEnumerable<TEntry> items );

      Task DeleteAsync( IEnumerable<TKey> ids, DateTime from, DateTime to );

      Task DeleteAsync( IEnumerable<TKey> ids );

      Task<MultiReadResult<TKey, TEntry>> ReadLatestAsync( IEnumerable<TKey> ids );

      Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, Sort sort = Sort.Descending );

      Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending );
   }
}
