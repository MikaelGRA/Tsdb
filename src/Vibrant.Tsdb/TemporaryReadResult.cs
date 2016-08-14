using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TemporaryReadResult<TKey, TEntry>
      where TEntry : IEntry
   {
      private Action _delete;

      public TemporaryReadResult( List<TEntry> entries, Action delete )
      {
         Entries = entries;
         _delete = delete;
      }

      public List<TEntry> Entries { get; private set; }

      public void Delete()
      {
         _delete();
      }
   }
}
