using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   internal class VolumeStorageLookupResult<TItem, TEntry> : StorageLookupResult<IVolumeStorage<TEntry>, TEntry, TItem>
      where TEntry : IEntry
   {
      public VolumeStorageLookupResult( IVolumeStorage<TEntry> storage )
         : base( storage )
      {

      }
   }
}
