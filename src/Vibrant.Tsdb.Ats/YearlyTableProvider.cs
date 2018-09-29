using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public class YearlyTableProvider<TKey> : ITableProvider<TKey>
   {
      private string _tableName;

      public YearlyTableProvider( string tableName )
      {
         _tableName = tableName;
      }

      public int GetMaxTableMisses( TKey id )
      {
         return 5;
      }

      public ITable GetPreviousTable( ITable currentTable )
      {
         return ( (YearlyTable)currentTable ).GetPrevious();
      }

      public ITable GetTable( TKey key, DateTime timestamp )
      {
         return new YearlyTable( timestamp.Year, _tableName );
      }

      public IEnumerable<ITable> IterateTables( TKey key, DateTime from, DateTime to )
      {
         var fromYear = from.Year;
         var toYear = to.Year;
         for( int current = toYear ; current >= fromYear ; current-- )
         {
            yield return new YearlyTable( current, _tableName );
         }
      }
   }
}
