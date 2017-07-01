using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Tests.Entries;
using Vibrant.Tsdb.InfluxDB;
using Vibrant.Tsdb.Sql;
using Vibrant.Tsdb.Tests.Model;

namespace Vibrant.Tsdb.Tests
{
   public class InfluxTypedStorageTests : AbstractTypedStorageTests<InfluxTypedStorage<string, BasicEntry, MeasureType>>
   {
      private static readonly string Endpoint;
      private static readonly string Database;
      private static readonly TestTypedKeyStorage KeyStorage;

      static InfluxTypedStorageTests()
      {
         var builder = new ConfigurationBuilder()
            .AddJsonFile( "appsettings.json" )
            .AddJsonFile( "appsettings.Hidden.json", true );
         var config = builder.Build();

         var ats = config.GetSection( "InfluxStorage" );
         Endpoint = ats.GetSection( "Endpoint" ).Value;
         Database = ats.GetSection( "Database" ).Value;

         KeyStorage = new TestTypedKeyStorage( new[]
         {
            "row1",
            "row2",
            "row3",
            "row4",
            "row5",
            "row6",
            "row7",
            "rowlol2",
            "rowlol3"
         } );
      }

      public override InfluxTypedStorage<string, BasicEntry, MeasureType> GetStorage( string tableName )
      {
         return new InfluxTypedStorage<string, BasicEntry, MeasureType>( new Uri( Endpoint ), Database, KeyStorage );
      }
   }
}
