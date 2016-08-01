using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
    internal class PerformanceStorageLookupResult<TLookup> : StorageLookupResult<IPerformanceStorage, TLookup>
    {
      public PerformanceStorageLookupResult( IPerformanceStorage storage )
         : base( storage )
      {

      }
   }
}
