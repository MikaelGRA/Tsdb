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
      private Action _delete;

      public TemporaryReadResult( List<ISerie<TKey, TEntry>> entries, Action delete )
      {
         Series = entries;
         _delete = delete;
      }

      public List<ISerie<TKey, TEntry>> Series { get; private set; }

      public void Delete()
      {
         _delete();
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
