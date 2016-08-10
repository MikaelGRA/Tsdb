using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   internal static class AtsKeyCalculator
   {
      private const string Seperator = "|";

      public static string CalculatePartitionKey<TKey, TEntry>( TEntry entry, IKeyConverter<TKey> keyConverter, IPartitionProvider<TKey> provider )
         where TEntry : IAtsEntry<TKey>
      {
         var key = entry.GetKey();
         return CalculatePartitionKey( keyConverter.Convert( key ), key, entry.GetTimestamp(), provider );
      }

      public static string CalculatePartitionKey( string id, string partitionRange )
      {
         StringBuilder builder = new StringBuilder();

         builder.Append( id );

         if( !string.IsNullOrEmpty( partitionRange ) )
         {
            builder.Append( Seperator )
               .Append( partitionRange );
         }

         return builder.ToString();
      }

      public static string CalculatePartitionKey<TKey>( string id, TKey key, DateTime timestamp, IPartitionProvider<TKey> provider )
      {
         StringBuilder builder = new StringBuilder();

         builder.Append( id );

         var partitionRange = provider.GetPartitioning( key, timestamp );
         if( !string.IsNullOrEmpty( partitionRange ) )
         {
            builder.Append( Seperator )
               .Append( partitionRange );
         }

         return builder.ToString();
      }

      public static string CalculateMaxPartitionKey<TKey>( string id, TKey key, IPartitionProvider<TKey> provider )
      {
         StringBuilder builder = new StringBuilder();

         builder.Append( id );

         var partitionRange = provider.GetMaxPartitioning( key );
         if( !string.IsNullOrEmpty( partitionRange ) )
         {
            builder.Append( Seperator )
               .Append( partitionRange );
         }

         return builder.ToString();
      }

      public static string CalculateMinPartitionKey<TKey>( string id, TKey key, IPartitionProvider<TKey> provider )
      {
         StringBuilder builder = new StringBuilder();

         builder.Append( id );

         var partitionRange = provider.GetMinPartitioning( key );
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
