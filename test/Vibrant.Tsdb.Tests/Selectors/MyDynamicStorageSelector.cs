using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Tests.Selectors
{
   public class MyDynamicStorageSelector<TKey, TEntry> : StorageSelectorBase<TKey, TEntry>
     where TEntry : IEntry
   {
      private StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>[] _selections;

      public MyDynamicStorageSelector( StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>[] selections )
      {
         _selections = selections;
      }

      protected override IEnumerable<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>> IterateAllStoragesFor( TKey key )
      {
         return _selections;
      }
   }
}
