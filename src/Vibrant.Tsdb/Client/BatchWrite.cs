using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   internal class BatchWrite<TKey, TEntry>
      where TEntry : IEntry
   {
      private TaskCompletionSource<bool> _tcs;
      private ISerie<TKey, TEntry> _series;

      public BatchWrite( ISerie<TKey, TEntry> series )
      {
         _tcs = new TaskCompletionSource<bool>();
         _series = series;
      }

      public void Complete()
      {
         _tcs.SetResult( true );
      }

      public void Fail( Exception e )
      {
         _tcs.SetException( e );
      }

      public void Add( ISerie<TKey, TEntry> serie )
      {
         var existingCollection = _series.GetEntries();
         foreach( var newEntry in serie.GetEntries() )
         {
            existingCollection.Add( newEntry );
         }
      }

      public int Count
      {
         get
         {
            return _series.GetEntries().Count;
         }
      }

      public ISerie<TKey, TEntry> GetBatch()
      {
         return _series;
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
