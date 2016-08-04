using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IPerformanceStorageSelector<TEntry> where TEntry : IEntry
   {
      IPerformanceStorage<TEntry> GetStorage( string id );
   }
}
