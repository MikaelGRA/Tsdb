using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Exceptions;

namespace Vibrant.Tsdb
{
   public static class TsdbTypeRegistry
   {
      private static readonly ConcurrentDictionary<int, Func<IEntry>> _entries = new ConcurrentDictionary<int, Func<IEntry>>();

      public static int MaxEntrySizeInBytes = 1024;

      public static void Register<TEntry>()
         where TEntry : IEntry, new()
      {
         var tmp = new TEntry();
         var code = tmp.GetTypeCode();

         Func<IEntry> ctor;
         if( !_entries.TryGetValue( code, out ctor ) )
         {
            _entries[ code ] = () => new TEntry();
         }
         else
         {
            var otherType = ctor();
            var thisType = new TEntry();

            if( otherType.GetType() != thisType.GetType() )
            {
               throw new TsdbException( $"The type '{otherType.GetType().FullName}' is attemping to use the type code {code} but it is already used by the type '{thisType.GetType().FullName}'." );
            }
         }
      }

      public static IEntry CreateEntry( int typeCode )
      {
         return _entries[ typeCode ]();
      }

      public static Func<IEntry> GetConstructor( int typeCode )
      {
         return _entries[ typeCode ];
      }
   }
}
