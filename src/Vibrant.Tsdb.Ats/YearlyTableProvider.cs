using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public class YearlyTableProvider : ITableProvider
   {
      public string GetPreviousTable( string currentTable )
      {
         return ( int.Parse( currentTable, CultureInfo.InvariantCulture ) - 1 ).ToString( CultureInfo.InvariantCulture );
      }

      public string GetTable( DateTime timestamp )
      {
         return timestamp.Year.ToString( CultureInfo.InvariantCulture );
      }

      public IEnumerable<string> IterateTables( DateTime from, DateTime to )
      {
         var fromYear = from.Year;
         var toYear = to.Year;
         for( int current = toYear ; current >= fromYear ; current-- )
         {
            yield return current.ToString( CultureInfo.InvariantCulture );
         }
      }
   }
}
