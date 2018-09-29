using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public interface ITableProvider<TKey>
   {
      int GetMaxTableMisses( TKey id );

      ITable GetTable( TKey key, DateTime timestamp );

      ITable GetPreviousTable( ITable currentTable );

      IEnumerable<ITable> IterateTables( TKey key, DateTime from, DateTime to );
   }
}
