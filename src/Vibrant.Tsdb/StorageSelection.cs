using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class StorageSelection<TKey, TEntry, TStorage>
      where TStorage : IStorage<TKey, TEntry>
      where TEntry : IEntry<TKey>
   {
      public StorageSelection( TStorage storage, DateTime? from, DateTime? to )
      {
         Storage = storage;
         From = from;
         To = to;
      }

      public StorageSelection( TStorage storage )
      {
         Storage = storage;
      }

      public TStorage Storage { get; private set; }

      public DateTime? From { get; private set; }

      public DateTime? To { get; private set; }
   }
}
