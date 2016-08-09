using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IVolumeStorageSelector<TKey, TEntry> where TEntry : IEntry<TKey>
   {
      IVolumeStorage<TKey, TEntry> GetStorage( TKey id );
   }
}
