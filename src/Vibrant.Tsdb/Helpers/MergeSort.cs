using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Helpers
{
   public class MergeSort
   {
      public static List<TEntry> Sort<TEntry>(
         IEnumerable<IEnumerable<TEntry>> collections,
         IComparer<TEntry> comparer,
         Func<IEnumerable<TEntry>, TEntry> resolveConflict )
      {
         var result = new List<TEntry>();

         var enumerators = collections.Select( x => x.GetEnumerator() ).ToList();

         // "initialize" each enumerator to point at first item
         for( int i = enumerators.Count - 1 ; i >= 0 ; i-- )
         {
            var enumerator = enumerators[ i ];
            var hasItems = enumerator.MoveNext();

            // if no items, simply remove the enumerator
            if( !hasItems )
            {
               enumerators.RemoveAt( i );
            }
         }

         // merge enumerators into result until complete
         while( enumerators.Count > 0 )
         {
            TEntry minimum = enumerators[ 0 ].Current;

            foreach( var enumerator in enumerators.Skip( 1 ) )
            {
               var current = enumerator.Current;

               if( comparer.Compare( current, minimum ) < 0 )
               {
                  minimum = current;
               }
            }

            var minimumItems = FindMinimumAndUpdateEnumerators( minimum, enumerators, comparer ).ToArray();

            var item = resolveConflict( minimumItems );

            result.Add( item );
         }

         return result;
      }

      private static IEnumerable<TEntry> FindMinimumAndUpdateEnumerators<TEntry>( 
         TEntry minimum, 
         List<IEnumerator<TEntry>> enumerators,
         IComparer<TEntry> comparer )
      {
         for( int i = enumerators.Count - 1 ; i >= 0 ; i-- )
         {
            var enumerator = enumerators[ i ];
            var current = enumerator.Current;
            
            if( comparer.Compare( current, minimum ) == 0 )
            {
               yield return current;

               var hasItems = enumerator.MoveNext();
               if( !hasItems )
               {
                  enumerators.RemoveAt( i );
               }
            }
         }
      }
   }
}
