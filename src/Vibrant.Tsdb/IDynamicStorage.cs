using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IDynamicStorage<TEntry> : IStorage<TEntry> where TEntry : IEntry
   {
      Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime to, Sort sort = Sort.Descending );

      Task<int> Delete( IEnumerable<string> ids, DateTime to );
   }
}
