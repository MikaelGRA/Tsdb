using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Vibrant.Tsdb.Ats.Serialization;

namespace Vibrant.Tsdb.Ats
{
   internal class TsdbTableEntity : TableEntity
   {
      public const int MaxByteCapacity = ( 64 * 1024 ) - 4;
      
      public byte[] P0 { get; set; }

      public TEntry[] GetEntries<TEntry>( string id, Sort sort )
         where TEntry : IAtsEntry, new()
      {
         return AtsSerializer.Deserialize<TEntry>( id, P0, sort );
      }

      /// <summary>
      /// Split the stream as a fat entity.
      /// </summary>
      public void SetData( byte[] data )
      {
         if( null == data ) throw new ArgumentNullException( "data" );
         if( data.Length >= MaxByteCapacity ) throw new ArgumentOutOfRangeException( "data" );

         P0 = data;
      }
   }
}
