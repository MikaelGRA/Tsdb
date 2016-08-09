using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IStorage<TKey, TEntry> where TEntry : IEntry<TKey>
   {
      Task Write( IEnumerable<TEntry> items );

      Task Delete( IEnumerable<TKey> ids, DateTime from, DateTime to );

      Task Delete( IEnumerable<TKey> ids );

      Task<MultiReadResult<TKey, TEntry>> ReadLatest( IEnumerable<TKey> ids );

      Task<MultiReadResult<TKey, TEntry>> Read( IEnumerable<TKey> ids, Sort sort = Sort.Descending );

      Task<MultiReadResult<TKey, TEntry>> Read( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending );
   }
}
