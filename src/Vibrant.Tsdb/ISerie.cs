using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ISerie<TKey, TEntry>
     where TEntry : IEntry
   {
      TKey GetKey();

      ICollection<TEntry> GetEntries();
   }
}
