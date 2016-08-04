using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Ats.Tests.Entries;
using Vibrant.Tsdb.Sql;

namespace Vibrant.Tsdb.Ats.Tests
{
    public class SqlStorageTests : AbstractStorageTests<SqlPerformanceStorage<BasicEntry>>
   {
      private static readonly string ConnectionString;

      static SqlStorageTests()
      {
         var builder = new ConfigurationBuilder()
            .AddJsonFile( "appsettings.json" )
            .AddJsonFile( "appsettings.Hidden.json", true );
         var config = builder.Build();

         var ats = config.GetSection( "SqlStorage" );
         ConnectionString = ats.GetSection( "ConnectionString" ).Value;
      }

      public override IStorage<BasicEntry> GetStorage( string tableName )
      {
         return new SqlPerformanceStorage<BasicEntry>( tableName, ConnectionString );
      }
   }
}
