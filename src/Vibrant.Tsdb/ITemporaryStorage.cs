using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ITemporaryStorage<TEntry>
      where TEntry : IEntry
   {
      void Write( IEnumerable<TEntry> entries );

      List<TEntry> Read( int count );

      // has more data???

      // continuous read???
   }
}
