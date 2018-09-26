//using Microsoft.Extensions.Configuration;
//using Vibrant.Tsdb.CosmosTables;
//using Vibrant.Tsdb.Tests.Entries;

//namespace Vibrant.Tsdb.Tests
//{
//   public class CosmosTablesStorageTests : AbstractStorageTests<CosmosTablesStorage<string, BasicEntry>>
//   {
//      private static readonly string ConnectionString;

//      static CosmosTablesStorageTests()
//      {
//         var builder = new ConfigurationBuilder()
//            .AddJsonFile( "appsettings.json" )
//            .AddJsonFile( "appsettings.Hidden.json", true );
//         var config = builder.Build();

//         var ats = config.GetSection( "CosmosStorage" );
//         ConnectionString = ats.GetSection( "ConnectionString" ).Value;
//      }

//      public override CosmosTablesStorage<string, BasicEntry> GetStorage( string tableName )
//      {
//         return new CosmosTablesStorage<string, BasicEntry>( tableName + "Dynamic", ConnectionString );
//      }
//   }
//}
