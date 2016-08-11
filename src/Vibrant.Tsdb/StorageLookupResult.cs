using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   internal class StorageLookupResult<TKey, TStorage, TEntry, TLookup>
      where TStorage : IStorage<TKey, TEntry>
      where TEntry : IEntry<TKey>
   {
      public StorageLookupResult( TStorage storage )
      {
         Lookups = new List<TLookup>();
         Storage = storage;
      }

      public StorageLookupResult( TStorage storage, DateTime? from, DateTime? to )
      {
         Lookups = new List<TLookup>();
         Storage = storage;
         From = from;
         To = to;
      }

      public List<TLookup> Lookups { get; private set; }

      public TStorage Storage { get; private set; }

      public DateTime? From { get; private set; }

      public DateTime? To { get; private set; }
   }
}
