using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   internal class VolumeStorageLookupResult<TKey, TItem, TEntry> : StorageLookupResult<TKey, IVolumeStorage<TKey, TEntry>, TEntry, TItem>
      where TEntry : IEntry
   {
      public VolumeStorageLookupResult( IVolumeStorage<TKey, TEntry> storage )
         : base( storage )
      {

      }

      public VolumeStorageLookupResult( IVolumeStorage<TKey, TEntry> storage, DateTime? from, DateTime? to ) : base( storage, from, to )
      {
      }
   }
}
