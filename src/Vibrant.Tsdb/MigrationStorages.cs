using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class MigrationStorages<TKey, TEntry>
      where TEntry : IEntry<TKey>
   {
      public MigrationStorages( IDynamicStorage<TKey, TEntry> dynamic, IVolumeStorage<TKey, TEntry> volume, DateTime? from, DateTime? to )
      {
         Dynamic = dynamic;
         Volume = volume;
         From = from;
         To = to;
      }

      public IDynamicStorage<TKey, TEntry> Dynamic { get; private set; }

      public IVolumeStorage<TKey, TEntry> Volume { get; private set; }

      public DateTime? From { get; private set; }

      public DateTime? To { get; private set; }
   }
}
