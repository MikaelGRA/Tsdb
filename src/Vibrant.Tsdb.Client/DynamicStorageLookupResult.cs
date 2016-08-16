using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
    internal class DynamicStorageLookupResult<TKey, TLookup, TEntry> : StorageLookupResult<TKey, IDynamicStorage<TKey, TEntry>, TEntry, TLookup>
      where TEntry : IEntry
    {
      public DynamicStorageLookupResult( IDynamicStorage<TKey, TEntry> storage )
         : base( storage )
      {

      }

      public DynamicStorageLookupResult( IDynamicStorage<TKey, TEntry> storage, DateTime? from, DateTime? to ) : base( storage, from, to )
      {
      }
   }
}
