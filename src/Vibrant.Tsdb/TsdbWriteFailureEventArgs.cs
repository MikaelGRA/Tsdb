using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbWriteFailureEventArgs<TKey, TEntry> : EventArgs
      where TEntry : IEntry<TKey>
   {
      private IEnumerable<TEntry> _entries;
      private Exception _exception;

      public TsdbWriteFailureEventArgs( IEnumerable<TEntry> entries, Exception exception )
      {
         _entries = entries;
         _exception = exception;
      }
      
      public IEnumerable<TEntry> Entries
      {
         get
         {
            return _entries;
         }
      }

      public Exception Exception
      {
         get
         {
            return _exception;
         }
      }
   }
}
