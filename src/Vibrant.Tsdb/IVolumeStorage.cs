using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IVolumeStorage<TEntry> : IStorage<TEntry> where TEntry : IEntry
   {
   }
}
