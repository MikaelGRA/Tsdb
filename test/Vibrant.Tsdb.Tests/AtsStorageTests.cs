using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Vibrant.Tsdb.Ats;
using Vibrant.Tsdb.Tests.Entries;

namespace Vibrant.Tsdb.Tests
{
   public class AtsStorageTests : AbstractStorageTests<AtsDynamicStorage<string, BasicEntry>>
   {
      private static readonly string ConnectionString;

      static AtsStorageTests()
      {
         var builder = new ConfigurationBuilder()
            .AddJsonFile( "appsettings.json" )
            .AddJsonFile( "appsettings.Hidden.json", true );
         var config = builder.Build();

         var ats = config.GetSection( "AtsStorage" );
         ConnectionString = ats.GetSection( "ConnectionString" ).Value;
      }

      public override AtsDynamicStorage<string, BasicEntry> GetStorage( string tableName )
      {
         return new AtsDynamicStorage<string, BasicEntry>( tableName + "Dynamic", ConnectionString );
      }
   }
}
