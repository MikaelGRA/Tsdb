using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   internal static class AtsKeyCalculator
   {
      private const string MinPartitionKeyRange = "9999";
      private const string MaxPartitionKeyRange = "0000";
      private const string Seperator = "|";

      public static string CalculatePartitionKey<TEntry>( TEntry entry, IPartitionProvider provider )
         where TEntry : IAtsEntry
      {
         return CalculatePartitionKey( entry.GetId(), entry.GetTimestamp(), provider );
      }

      public static string CalculatePartitionKey( string id, DateTime timestamp, IPartitionProvider provider )
      {
         StringBuilder builder = new StringBuilder();

         builder.Append( id );

         var partitionRange = provider.GetPartitioning( id, timestamp );
         if( !string.IsNullOrEmpty( partitionRange ) )
         {
            builder.Append( Seperator )
               .Append( partitionRange );
         }

         return builder.ToString();
      }

      public static string CalculateMaxPartitionKey( string id, IPartitionProvider provider )
      {
         StringBuilder builder = new StringBuilder();

         builder.Append( id );

         var partitionRange = provider.GetMaxPartitioning( id );
         if( !string.IsNullOrEmpty( partitionRange ) )
         {
            builder.Append( Seperator )
               .Append( partitionRange );
         }

         return builder.ToString();
      }

      public static string CalculateMinPartitionKey( string id, IPartitionProvider provider )
      {
         StringBuilder builder = new StringBuilder();

         builder.Append( id );

         var partitionRange = provider.GetMinPartitioning( id );
         if( !string.IsNullOrEmpty( partitionRange ) )
         {
            builder.Append( Seperator )
               .Append( partitionRange );
         }

         return builder.ToString();
      }

      public static string CalculateRowKey( DateTime from )
      {
         return ( DateTime.MaxValue.Ticks - from.Ticks ).ToString();
      }
   }
}
