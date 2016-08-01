using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   internal class StorageLookupResult<TStorage, TItem>
      where TStorage : IStorage
    {
      public StorageLookupResult( TStorage storage )
      {
         Lookups = new List<TItem>();
         Storage = storage;
      }

      public List<TItem> Lookups { get; private set; }

      public TStorage Storage { get; private set; }
   }
}
