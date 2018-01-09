using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.CosmosTables
{
   // TODO: Implement iterable version

   public class YearlyPartitioningProvider<TKey> : IIterablePartitionProvider<TKey>
   {
      private static readonly string MinPartitionKeyRange = "9999";
      private static readonly string MaxPartitionKeyRange = "0000";

      public string GetMaxPartitioning( TKey id )
      {
         return MaxPartitionKeyRange;
      }

      public string GetMinPartitioning( TKey id )
      {
         return MinPartitionKeyRange;
      }

      public string GetPartitioning( TKey id, DateTime timestamp )
      {
         return CalculatePartitionKeyRange( timestamp );
      }

      private static string CalculatePartitionKeyRange( DateTime timestamp )
      {
         return CalculatePartitionKeyRange( timestamp.Year );
      }

      private static string CalculatePartitionKeyRange( int year )
      {
         var inverseYear = 9999 - year;
         if( inverseYear < 1000 )
         {
            return inverseYear.ToString( "0000" );
         }
         return inverseYear.ToString();
      }

      public IEnumerable<string> IteratePartitions( TKey key, DateTime from, DateTime to )
      {
         var fromYear = from.Year;
         var toYear = to.Year;
         for( int current = toYear ; current >= fromYear ; current-- )
         {
            yield return CalculatePartitionKeyRange( current );
         }
      }
   }
}
