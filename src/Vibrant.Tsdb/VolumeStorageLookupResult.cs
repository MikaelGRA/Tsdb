using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
    public class VolumeStorageLookupResult<TItem> : StorageLookupResult<IVolumeStorage, TItem>
    {
      public VolumeStorageLookupResult( IVolumeStorage storage )
         : base( storage )
      {

      }
   }
}
