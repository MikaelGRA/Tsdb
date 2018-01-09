using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.CosmosDB.Table;
using Vibrant.Tsdb.CosmosTables.Serialization;

namespace Vibrant.Tsdb.CosmosTables
{
   internal class TsdbTableEntity : TableEntity
   {
      public const int MaxByteCapacity = ( 64 * 1024 ) - 4;
      
      public byte[] P0 { get; set; }

      public TEntry[] GetEntries<TKey, TEntry>( Sort sort )
         where TEntry : ICosmosTablesEntry, new()
      {
         return CosmosTablesSerializer.Deserialize<TKey, TEntry>( P0, sort );
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
