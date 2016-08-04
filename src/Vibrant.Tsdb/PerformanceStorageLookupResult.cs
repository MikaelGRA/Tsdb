using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
    internal class PerformanceStorageLookupResult<TLookup, TEntry> : StorageLookupResult<IPerformanceStorage<TEntry>, TEntry, TLookup>
      where TEntry : IEntry
    {
      public PerformanceStorageLookupResult( IPerformanceStorage<TEntry> storage )
         : base( storage )
      {

      }
   }
}
