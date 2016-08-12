using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Tests.Selectors
{
   public class MyVolumeStorageSelector<TKey, TEntry> : VolumeStorageSelectorBase<TKey, TEntry>
     where TEntry : IEntry<TKey>
   {
      private StorageSelection<TKey, TEntry, IVolumeStorage<TKey, TEntry>>[] _selections;

      public MyVolumeStorageSelector( StorageSelection<TKey, TEntry, IVolumeStorage<TKey, TEntry>>[] selections )
      {
         _selections = selections;
      }

      protected override IEnumerable<StorageSelection<TKey, TEntry, IVolumeStorage<TKey, TEntry>>> IterateAllStoragesFor( TKey key )
      {
         return _selections;
      }
   }
}
