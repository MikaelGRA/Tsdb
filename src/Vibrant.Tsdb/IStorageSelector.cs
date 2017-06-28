using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IStorageSelector<TKey, TEntry> where TEntry : IEntry
   {
      IEnumerable<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>> GetStorage( TKey id, DateTime? from, DateTime? to );

      IStorage<TKey, TEntry> GetStorage( TKey key, TEntry entry );
   }
}
