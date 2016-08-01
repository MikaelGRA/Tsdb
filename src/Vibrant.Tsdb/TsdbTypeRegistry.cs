using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Exceptions;

namespace Vibrant.Tsdb
{
   /// <summary>
   /// Type registry for all implementation of IEntry that 
   /// should can be used.
   /// </summary>
   public static class TsdbTypeRegistry
   {
      private static readonly ConcurrentDictionary<int, Func<IEntry>> _entries = new ConcurrentDictionary<int, Func<IEntry>>();

      /// <summary>
      /// Gets or sets the max size of an entry in bytes.
      /// </summary>
      public static int MaxEntrySizeInBytes = 1024;

      /// <summary>
      /// Registers the specified implementation of IEntry.
      /// </summary>
      /// <typeparam name="TEntry"></typeparam>
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

      /// <summary>
      /// Creates an instance of IEntry identified by the given type code.
      /// </summary>
      /// <param name="typeCode"></param>
      /// <returns></returns>
      public static IEntry CreateEntry( ushort typeCode )
      {
         return _entries[ typeCode ]();
      }
   }
}
