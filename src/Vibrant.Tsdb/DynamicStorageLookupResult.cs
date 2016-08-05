using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
    internal class DynamicStorageLookupResult<TLookup, TEntry> : StorageLookupResult<IDynamicStorage<TEntry>, TEntry, TLookup>
      where TEntry : IEntry
    {
      public DynamicStorageLookupResult( IDynamicStorage<TEntry> storage )
         : base( storage )
      {

      }
   }
}
