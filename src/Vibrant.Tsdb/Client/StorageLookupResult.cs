using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   internal class StorageLookupResult<TKey, TLookup, TEntry> : StorageLookupResultBase<TKey, IStorage<TKey, TEntry>, TEntry, TLookup>
     where TEntry : IEntry
   {
      public StorageLookupResult( IStorage<TKey, TEntry> storage )
         : base( storage )
      {

      }

      public StorageLookupResult( IStorage<TKey, TEntry> storage, DateTime? from, DateTime? to ) : base( storage, from, to )
      {
      }
   }
}
