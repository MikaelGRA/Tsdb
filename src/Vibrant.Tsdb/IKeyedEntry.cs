using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IKeyedEntry<TKey> : IEntry
   {
      TKey GetKey();
   }
}
