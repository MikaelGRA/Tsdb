using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ITemporaryStorage<TKey, TEntry>
      where TEntry : IEntry
   {
      Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> entries );

      Task<TemporaryReadResult<TKey, TEntry>> ReadAsync( int count );

      Task DeleteAsync();
   }
}
