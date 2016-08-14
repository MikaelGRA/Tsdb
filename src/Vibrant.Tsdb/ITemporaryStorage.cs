using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface ITemporaryStorage<TKey, TEntry>
      where TEntry : IEntry
   {
      void Write( IEnumerable<TEntry> entries );

      TemporaryReadResult<TKey, TEntry> Read( int count );

      void Delete();
   }
}
