using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   internal static class AtsKeyCalculator
   {
      private static readonly string MinPartitionKeyRange = "9999";
      private static readonly string MaxPartitionKeyRange = "0000";

      public static string CalcuatePartitionKey<TEntry>( TEntry entity )
         where TEntry : IAtsEntry
      {
         StringBuilder builder = new StringBuilder();

         builder.Append( entity.GetId() )
            .Append( "|" )
            .Append( CalculatePartitionKeyRange( entity.GetTimestamp() ) );

         return builder.ToString();
      }

      public static string CalculatePartitionKey( string id, DateTime timestamp )
      {
         StringBuilder builder = new StringBuilder();

         builder.Append( id )
            .Append( "|" )
            .Append( CalculatePartitionKeyRange( timestamp ) );

         return builder.ToString();
      }

      public static string CalculateMaxPartitionKey( string id )
      {
         StringBuilder builder = new StringBuilder();

         builder.Append( id )
            .Append( "|" )
            .Append( MaxPartitionKeyRange );

         return builder.ToString();
      }

      public static string CalculateMinPartitionKey( string id )
      {
         StringBuilder builder = new StringBuilder();

         builder.Append( id )
            .Append( "|" )
            .Append( MinPartitionKeyRange );

         return builder.ToString();
      }

      public static string CalculateRowKey( DateTime from )
      {
         return ( DateTime.MaxValue.Ticks - from.Ticks ).ToString();
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
   }
}
