using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TemporaryReadResult<TKey, TEntry> : IEnumerable<ISerie<TKey, TEntry>>
      where TEntry : IEntry
   {
      private Func<Task> _delete;

      public TemporaryReadResult( List<Serie<TKey, TEntry>> entries, Func<Task> delete )
      {
         Series = entries;
         _delete = delete;
      }

      public List<Serie<TKey, TEntry>> Series { get; private set; }

      public Task DeleteAsync()
      {
         return _delete();
      }

      public IEnumerator<ISerie<TKey, TEntry>> GetEnumerator()
      {
         return Series.GetEnumerator();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return Series.GetEnumerator();
      }
   }
}
