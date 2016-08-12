using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Tests.Selectors
{
   public class MyDynamicStorageSelector<TKey, TEntry> : DynamicStorageSelectorBase<TKey, TEntry>
     where TEntry : IEntry<TKey>
   {
      private StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>[] _selections;

      public MyDynamicStorageSelector( StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>[] selections )
      {
         _selections = selections;
      }

      protected override IEnumerable<StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>> IterateAllStoragesFor( TKey key )
      {
         return _selections;
      }
   }
}
