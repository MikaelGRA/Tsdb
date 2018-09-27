using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class ReadResultExtensions
   {
      public static MultiReadResult<TKey, TEntry> Combine<TKey, TEntry>( this IEnumerable<MultiReadResult<TKey, TEntry>> that, int count )
         where TEntry : IEntry
      {
         if( count == 1 ) return that.First();

         var dictionary = CreateDictionary( that.SelectMany( x => x ) );
         var result = new MultiReadResult<TKey, TEntry>();
         foreach( var kvp in dictionary )
         {
            var key = kvp.Key;
            var values = kvp.Value;
            var sort = values[ 0 ].Sort;
            var readResult = new ReadResult<TKey, TEntry>( key, sort, values );
            result.AddResult( readResult );
         }
         return result;
      }

      public static MultiReadResult<TKey, TEntry> Combine<TKey, TEntry>( this IEnumerable<ReadResult<TKey, TEntry>> that, int count )
         where TEntry : IEntry
      {
         if( count == 1 ) return new MultiReadResult<TKey, TEntry>( that );

         var dictionary = CreateDictionary( that );
         var result = new MultiReadResult<TKey, TEntry>();
         foreach( var kvp in dictionary )
         {
            var key = kvp.Key;
            var values = kvp.Value;
            var sort = values[ 0 ].Sort;
            var readResult = new ReadResult<TKey, TEntry>( key, sort, values );
            result.AddResult( readResult );
         }
         return result;
      }

      private static Dictionary<TKey, List<ReadResult<TKey, TEntry>>> CreateDictionary<TKey, TEntry>( IEnumerable<ReadResult<TKey, TEntry>> that )
         where TEntry : IEntry
      {
         Dictionary<TKey, List<ReadResult<TKey, TEntry>>> results = new Dictionary<TKey, List<ReadResult<TKey, TEntry>>>();

         foreach( var result in that )
         {
            if( !results.TryGetValue( result.Key, out var value ) )
            {
               value = new List<ReadResult<TKey, TEntry>>();
               results.Add( result.Key, value );
            }
            value.Add( result );
         }

         return results;
      }
   }
}
