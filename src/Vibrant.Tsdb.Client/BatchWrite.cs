using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   internal class BatchWrite<TKey, TEntry>
      where TEntry : IEntry
   {
      private TaskCompletionSource<bool> _tcs;
      private List<TEntry> _entries;

      public BatchWrite()
      {
         _tcs = new TaskCompletionSource<bool>();
         _entries = new List<TEntry>();
      }

      public void Complete()
      {
         _tcs.SetResult( true );
      }

      public void Fail( Exception e )
      {
         _tcs.SetException( e );
      }

      public void Add( IEnumerable<TEntry> entries )
      {
         _entries.AddRange( entries );
      }

      public List<TEntry> Entries
      {
         get
         {
            return _entries;
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
