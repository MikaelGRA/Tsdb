using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class VolumeStorageSelection<TKey, TEntry> : StorageSelection<TKey, TEntry, IVolumeStorage<TKey, TEntry>>
      where TEntry : IEntry<TKey>
   {
      public VolumeStorageSelection( IVolumeStorage<TKey, TEntry> storage ) : base( storage )
      {
      }

      public VolumeStorageSelection( IVolumeStorage<TKey, TEntry> storage, DateTime? from, DateTime? to ) : base( storage, from, to )
      {
      }
   }
}
