using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IVolumeStorage<TKey, TEntry> : IStorage<TKey, TEntry> where TEntry : IEntry<TKey>
   {
   }
}
