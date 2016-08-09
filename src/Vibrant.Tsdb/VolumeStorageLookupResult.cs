using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   internal class VolumeStorageLookupResult<TKey, TItem, TEntry> : StorageLookupResult<TKey, IVolumeStorage<TKey, TEntry>, TEntry, TItem>
      where TEntry : IEntry<TKey>
   {
      public VolumeStorageLookupResult( IVolumeStorage<TKey, TEntry> storage )
         : base( storage )
      {

      }
   }
}
