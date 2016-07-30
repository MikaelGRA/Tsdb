using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class MultiReadResult<TEntry> : IEnumerable<ReadResult<TEntry>>
      where TEntry : IEntry
   {
      private IDictionary<string, ReadResult<TEntry>> _results;

      public MultiReadResult( IDictionary<string, ReadResult<TEntry>> results )
      {
         _results = results;
      }

      public ReadResult<TEntry> FindResult( string id )
      {
         return _results[ id ];
      }

      public IEnumerator<ReadResult<TEntry>> GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }
   }
}
