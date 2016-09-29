using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public class YearlyTableProvider : ITableProvider
   {
      public ITable GetPreviousTable( ITable currentTable )
      {
         return ( (YearlyTable)currentTable ).GetPrevious();
      }

      public ITable GetTable( DateTime timestamp )
      {
         return new YearlyTable( timestamp.Year );
      }

      public IEnumerable<ITable> IterateTables( DateTime from, DateTime to )
      {
         var fromYear = from.Year;
         var toYear = to.Year;
         for( int current = toYear ; current >= fromYear ; current-- )
         {
            yield return new YearlyTable( current );
         }
      }
   }
}
