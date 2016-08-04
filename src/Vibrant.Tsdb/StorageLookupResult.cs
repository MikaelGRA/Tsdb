using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   internal class StorageLookupResult<TStorage, TEntry, TLookup>
      where TStorage : IStorage<TEntry>
      where TEntry : IEntry
    {
      public StorageLookupResult( TStorage storage )
      {
         Lookups = new List<TLookup>();
         Storage = storage;
      }

      public List<TLookup> Lookups { get; private set; }

      public TStorage Storage { get; private set; }
   }
}
