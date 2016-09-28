using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public interface ITableProvider
   {
      string GetTable( DateTime timestamp );

      string GetPreviousTable( string currentTable );

      IEnumerable<string> IterateTables( DateTime from, DateTime to );
   }
}
