using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Ats.Tests.Entries;
using Vibrant.Tsdb.InfluxDB;
using Vibrant.Tsdb.Sql;

namespace Vibrant.Tsdb.Ats.Tests
{
   public class InfluxStorageTests : AbstractDynamicStorageTests<InfluxDynamicStorage<BasicEntry>>
   {
      private static readonly string Endpoint;
      private static readonly string Database;

      static InfluxStorageTests()
      {
         var builder = new ConfigurationBuilder()
            .AddJsonFile( "appsettings.json" )
            .AddJsonFile( "appsettings.Hidden.json", true );
         var config = builder.Build();

         var ats = config.GetSection( "InfluxStorage" );
         Endpoint = ats.GetSection( "Endpoint" ).Value;
         Database = ats.GetSection( "Database" ).Value;
      }

      public override InfluxDynamicStorage<BasicEntry> GetStorage( string tableName )
      {
         return new InfluxDynamicStorage<BasicEntry>( new Uri( Endpoint ), Database );
      }
   }
}
