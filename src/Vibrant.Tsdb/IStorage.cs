using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IStorage
   {
      Task Write( IEnumerable<IEntry> items );

      Task<int> Delete( IEnumerable<string> ids, DateTime from, DateTime to );

      Task<int> Delete( IEnumerable<string> ids );

      Task<MultiReadResult<IEntry>> ReadLatest( IEnumerable<string> ids );

      Task<MultiReadResult<IEntry>> Read( IEnumerable<string> ids, Sort sort = Sort.Descending );

      Task<MultiReadResult<IEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending );
   }
}
