using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class DynamicStorageSelection<TKey, TEntry> : StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>
      where TEntry : IEntry<TKey>
   {
      public DynamicStorageSelection( IDynamicStorage<TKey, TEntry> storage ) : base( storage )
      {
      }

      public DynamicStorageSelection( IDynamicStorage<TKey, TEntry> storage, DateTime? from, DateTime? to ) : base( storage, from, to )
      {
      }
   }
}
