using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   internal class StorageLookupResult<TKey, TStorage, TEntry, TLookup>
      where TStorage : IStorage<TKey, TEntry>
      where TEntry : IEntry
   {
      public StorageLookupResult( TStorage storage )
      {
         Storage = storage;
      }

      public StorageLookupResult( TStorage storage, DateTime? from, DateTime? to )
      {
         Storage = storage;
         From = from;
         To = to;
      }

      public TLookup Lookups { get; set; }

      public TStorage Storage { get; private set; }

      public DateTime? From { get; private set; }

      public DateTime? To { get; private set; }
   }
}
