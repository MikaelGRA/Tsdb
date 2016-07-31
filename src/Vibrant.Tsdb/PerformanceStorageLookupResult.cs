using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
    public class PerformanceStorageLookupResult<TLookup> : StorageLookupResult<IPerformanceStorage, TLookup>
    {
      public PerformanceStorageLookupResult( IPerformanceStorage storage )
         : base( storage )
      {

      }
   }
}
