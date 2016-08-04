using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IVolumeStorageSelector<TEntry> where TEntry : IEntry
   {
      IVolumeStorage<TEntry> GetStorage( string id );
   }
}
