using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   internal class BatchWrite
   {
      private TaskCompletionSource<bool> _tcs;
      private List<IEnumerable<IEntry>> _entries;

      public BatchWrite()
      {
         _tcs = new TaskCompletionSource<bool>();
         _entries = new List<IEnumerable<IEntry>>();
      }

      public void Complete()
      {
         _tcs.SetResult( true );
      }

      public void Fail( Exception e )
      {
         _tcs.SetException( e );
      }

      public void Add( IEnumerable<IEntry> entries )
      {
         _entries.Add( entries );
      }

      public IEnumerable<IEntry> Entries
      {
         get
         {
            return _entries.SelectMany( x => x );
         }
      }
      
      public Task Task
      {
         get
         {
            return _tcs.Task;
         }
      }
   }
}
