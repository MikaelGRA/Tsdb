using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IDynamicStorageSelector<TEntry> where TEntry : IEntry
   {
      IDynamicStorage<TEntry> GetStorage( string id );
   }
}
