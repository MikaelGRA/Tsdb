using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IPerformanceStorage : IStorage
   {
      Task<MultiReadResult<IEntry>> Read( IEnumerable<string> ids, DateTime to, Sort sort = Sort.Descending );

      Task<int> Delete( IEnumerable<string> ids, DateTime to );
   }
}
