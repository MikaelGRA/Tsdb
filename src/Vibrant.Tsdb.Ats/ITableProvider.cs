using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public interface ITableProvider
   {
      int MaxTableMisses { get; }

      ITable GetTable( DateTime timestamp );

      ITable GetPreviousTable( ITable currentTable );

      IEnumerable<ITable> IterateTables( DateTime from, DateTime to );
   }
}
