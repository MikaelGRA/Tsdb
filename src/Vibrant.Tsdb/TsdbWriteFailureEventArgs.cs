using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbWriteFailureEventArgs<TEntry> : EventArgs
      where TEntry : IEntry
   {
      private IEnumerable<TEntry> _entries;

      public TsdbWriteFailureEventArgs( IEnumerable<TEntry> entries )
      {
         _entries = entries;
      }
      
      public IEnumerable<TEntry> Entries
      {
         get
         {
            return _entries;
         }
      }
   }
}
